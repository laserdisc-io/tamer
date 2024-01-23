package tamer
package s3

import java.time.{Duration, Instant, ZoneId}
import java.time.format.DateTimeFormatter

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._
import zio.s3._
import zio.stream.ZPipeline

import scala.math.Ordering.Implicits.infixOrderingOps

sealed abstract case class S3Setup[R, K: Tag, V: Tag, SV: Tag: Hashable](
    initialState: SV,
    recordKey: (SV, V) => K,
    bucket: String,
    prefix: String,
    parallelism: Int,
    minimumIntervalForBucketFetch: Duration,
    maximumIntervalForBucketFetch: Duration,
    selectObjectForState: (SV, Keys) => Option[String],
    stateFold: (KeysR, SV, Queue[Unit]) => UIO[SV],
    pipeline: ZPipeline[R, Throwable, Byte, V]
)(
    implicit ev: SerdesProvider[K, V, SV]
) extends Setup[R with S3, K, V, SV] {

  private[this] sealed trait EphemeralChange extends Product with Serializable
  private[this] object EphemeralChange {
    case object Detected    extends EphemeralChange
    case object NotDetected extends EphemeralChange

    def apply(b: Boolean): EphemeralChange = if (b) Detected else NotDetected
  }

  private[this] final val bucketHash       = bucket.hash
  private[this] final val prefixHash       = prefix.hash
  private[this] final val initialStateHash = initialState.hash

  override final val stateKey = bucketHash + prefixHash + initialStateHash
  override final val repr =
    s"""bucket:      $bucket
       |bucket hash: $bucketHash
       |prefix:      $prefix
       |prefix hash: $prefixHash
       |parallelism: $parallelism
       |""".stripMargin

  private final val logTask = log4sFromName.provideEnvironment(ZEnvironment("tamer.s3"))

  private final val initialEphemeralState = Ref.make(List.empty[String])

  private final val fetchSchedule           = Schedule.exponential(minimumIntervalForBucketFetch) || Schedule.spaced(maximumIntervalForBucketFetch)
  private final val ephemeralChangeSchedule = Schedule.once ++ fetchSchedule.untilInput((_: EphemeralChange) == EphemeralChange.Detected)

  private final def updatedSourceState(keysR: KeysR, keysChangedToken: Queue[Unit]): ZIO[S3, Throwable, EphemeralChange] = {
    val paginationMaxKeys         = 1000L      // FIXME magic
    val paginationMaxPages        = 1000L      // FIXME magic
    val defaultTimeoutBucketFetch = 60.seconds // FIXME magic
    val timeoutForFetchAllKeys =
      if (minimumIntervalForBucketFetch < defaultTimeoutBucketFetch) minimumIntervalForBucketFetch
      else defaultTimeoutBucketFetch

    def warnAboutSpuriousKeys(log: LogWriter[Task], keyList: List[String]) = {
      val witness = keyList.find(!_.startsWith(prefix)).getOrElse("")
      log.warn(s"""server returned '$witness' (and maybe more files) which don't match prefix "$prefix"""")
    }

    for {
      log                    <- logTask
      _                      <- log.info(s"getting list of keys in bucket $bucket with prefix $prefix")
      initialObjListing      <- listObjects(bucket, ListObjectOptions.from(prefix, paginationMaxKeys))
      allObjListings         <- paginate(initialObjListing).take(paginationMaxPages).timeout(timeoutForFetchAllKeys).runCollect.map(_.toList)
      keyList                <- ZIO.succeed(allObjListings.flatMap(_.objectSummaries.map(_.key)).sorted)
      cleanKeyList           <- ZIO.succeed(keyList.filter(_.startsWith(prefix)))
      _                      <- ZIO.when(keyList.size != cleanKeyList.size)(warnAboutSpuriousKeys(log, keyList))
      _                      <- log.debug(s"current key list has ${cleanKeyList.length} elements")
      _                      <- log.debug(s"the first and last elements are ${cleanKeyList.headOption} and ${cleanKeyList.lastOption}")
      previousListOfKeys     <- keysR.getAndSet(cleanKeyList)
      detectedKeyListChanged <- ZIO.succeed(cleanKeyList != previousListOfKeys)
      _                      <- ZIO.when(detectedKeyListChanged)(log.debug("detected change in key list") *> keysChangedToken.offer(()))
    } yield EphemeralChange(detectedKeyListChanged)
  }

  protected final def process(keysR: KeysR, keysChangedToken: Queue[Unit], currentState: SV, queue: Enqueue[NonEmptyChunk[(K, V)]]) = for {
    log       <- logTask
    nextState <- stateFold(keysR, currentState, keysChangedToken)
    _         <- log.debug(s"next state computed to be $nextState")
    keys      <- keysR.get
    optKey    <- ZIO.succeed(selectObjectForState(nextState, keys))
    _ <- log.debug(s"will ask for key $optKey") *> optKey
      .map(
        getObject(bucket, _)
          .via(pipeline)
          .map(value => recordKey(nextState, value) -> value)
          .runForeachChunk(chunk => NonEmptyChunk.fromChunk(chunk).map(queue.offer).getOrElse(ZIO.unit))
      )
      .getOrElse(ZIO.fail(TamerError(s"File not found with key $optKey for state $nextState"))) // FIXME: relies on nextState.toString
  } yield nextState

  override def iteration(currentState: SV, queue: Enqueue[NonEmptyChunk[(K, V)]]): RIO[R with S3, SV] = for {
    sourceState <- initialEphemeralState
    token       <- Queue.dropping[Unit](requestedCapacity = 1)
    _           <- updatedSourceState(sourceState, token).scheduleFrom(EphemeralChange.Detected)(ephemeralChangeSchedule).forever.fork
    newState    <- process(sourceState, token, currentState, queue)
  } yield newState
}

object S3Setup {
  private[this] final val defaultPipeline = ZPipeline.utf8Decode >>> ZPipeline.splitLines

  def apply[R, K: Tag, V: Tag, SV: Tag: Hashable](
      bucket: String,
      prefix: String,
      minimumIntervalForBucketFetch: Duration,
      maximumIntervalForBucketFetch: Duration,
      initialState: SV
  )(
      recordKey: (SV, V) => K,
      selectObjectForState: (SV, Keys) => Option[String],
      stateFold: (KeysR, SV, Queue[Unit]) => UIO[SV],
      parallelism: Int = 1,
      pipeline: ZPipeline[R, Throwable, Byte, V] = defaultPipeline
  )(
      implicit ev: SerdesProvider[K, V, SV]
  ): S3Setup[R, K, V, SV] = new S3Setup(
    initialState,
    recordKey,
    bucket,
    prefix,
    parallelism,
    minimumIntervalForBucketFetch,
    maximumIntervalForBucketFetch,
    selectObjectForState,
    stateFold,
    pipeline
  ) {}

  private[s3] final def suffixWithoutFileExtension(key: String, prefix: String, formatter: ZonedDateTimeFormatter): String = {
    val dotCountInDate = formatter.format(Instant.EPOCH).count(_ == '.')
    val keyWithoutExtension =
      if (key.count(_ == '.') > dotCountInDate) key.split('.').splitAt(dotCountInDate + 1)._1.mkString(".") else key
    keyWithoutExtension.stripPrefix(prefix)
  }

  private[s3] final def parseInstantFromKey(key: String, prefix: String, formatter: ZonedDateTimeFormatter): Instant =
    Instant.from(formatter.parse(suffixWithoutFileExtension(key, prefix, formatter)))

  private final def getNextInstant(keysR: KeysR, from: Instant, prefix: String, formatter: ZonedDateTimeFormatter): UIO[Option[Instant]] =
    keysR.get.map(_.map(parseInstantFromKey(_, prefix, formatter)).filter(_ > from).sorted.headOption)

  private[s3] final def getNextState(prefix: String, formatter: ZonedDateTimeFormatter)(
      keysR: KeysR,
      from: Instant,
      keysChangedToken: Queue[Unit]
  ): UIO[Instant] = getNextInstant(keysR, from, prefix, formatter).flatMap {
    case Some(newInstant) if newInstant > from => ZIO.succeed(newInstant)
    case _                                     => keysChangedToken.take *> getNextState(prefix, formatter)(keysR, from, keysChangedToken)
  }

  private final def selectObjectForInstant(formatter: ZonedDateTimeFormatter)(from: Instant, keys: Keys): Option[String] =
    keys.find(_.contains(formatter.format(from)))

  final def timed[R, K: Tag, V: Tag](
      bucket: String,
      prefix: String,
      from: Instant,
      recordKey: (Instant, V) => K = (l: Instant, _: V) => l,
      pipeline: ZPipeline[R, Throwable, Byte, V] = defaultPipeline,
      parallelism: Int = 1,
      dateTimeFormatter: ZonedDateTimeFormatter = ZonedDateTimeFormatter(DateTimeFormatter.ISO_INSTANT, ZoneId.systemDefault()),
      minimumIntervalForBucketFetch: Duration = 5.minutes,
      maximumIntervalForBucketFetch: Duration = 5.minutes
  )(
      implicit ev: SerdesProvider[K, V, Instant]
  ): S3Setup[R, K, V, Instant] = new S3Setup(
    from,
    recordKey,
    bucket,
    prefix,
    parallelism,
    minimumIntervalForBucketFetch,
    maximumIntervalForBucketFetch,
    selectObjectForInstant(dateTimeFormatter) _,
    getNextState(prefix, dateTimeFormatter) _,
    pipeline
  ) {}
}
