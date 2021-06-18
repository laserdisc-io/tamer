package tamer
package s3

import java.time.{Duration, Instant, ZoneId}
import java.time.format.DateTimeFormatter

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._
import zio.clock.Clock
import zio.duration.durationInt
import zio.s3._
import zio.stream.ZTransducer

import scala.math.Ordering.Implicits.infixOrderingOps

sealed abstract case class S3Setup[R, K, V, S: Hashable](
    bucketName: String,
    prefix: String,
    parallelism: Int,
    minimumIntervalForBucketFetch: Duration,
    maximumIntervalForBucketFetch: Duration,
    serdes: Setup.Serdes[K, V, S],
    defaultState: S,
    recordKey: (S, V) => K,
    selectObjectForState: (S, Keys) => Option[String],
    stateFold: (KeysR, S, Queue[Unit]) => UIO[S],
    transducer: ZTransducer[R, Throwable, Byte, V]
) extends Setup[R with Clock with S3, K, V, S] {

  private[this] sealed trait EphemeralChange extends Product with Serializable
  private[this] final object EphemeralChange {
    final case object Detected    extends EphemeralChange
    final case object NotDetected extends EphemeralChange

    def apply(b: Boolean): EphemeralChange = if (b) Detected else NotDetected
  }

  override final val stateKey = bucketName.hash + prefix.hash + defaultState.hash
  override final val repr =
    s"""bucket:      $bucketName
       |prefix:      $prefix
       |parallelism: $parallelism
       |""".stripMargin

  private final val logTask = log4sFromName.provide("tamer.s3")

  private final val initialEphemeralState = Ref.make(List.empty[String])

  private final val fetchSchedule = Schedule.exponential(minimumIntervalForBucketFetch) || Schedule.spaced(maximumIntervalForBucketFetch)

  private final def updatedSourceState(keysR: KeysR, keysChangedToken: Queue[Unit]) = {
    val paginationMaxKeys         = 1000L
    val paginationMaxPages        = 1000L
    val defaultTimeoutBucketFetch = 60.seconds
    val timeoutForFetchAllKeys =
      if (minimumIntervalForBucketFetch < defaultTimeoutBucketFetch) minimumIntervalForBucketFetch
      else defaultTimeoutBucketFetch

    def warnAboutSpuriousKeys(log: LogWriter[Task], keyList: List[String]) = {
      val witness = keyList.find(!_.startsWith(prefix)).getOrElse("")
      log.warn(s"""Server returned '$witness' (and maybe more files) which don't match prefix "$prefix"""")
    }

    for {
      log                    <- logTask
      _                      <- log.info(s"getting list of keys in bucket $bucketName with prefix $prefix")
      initialObjListing      <- listObjects(bucketName, ListObjectOptions.from(prefix, paginationMaxKeys))
      allObjListings         <- paginate(initialObjListing).take(paginationMaxPages).timeout(timeoutForFetchAllKeys).runCollect.map(_.toList)
      keyList                <- UIO(allObjListings.flatMap(_.objectSummaries.map(_.key)).sorted)
      cleanKeyList           <- UIO(keyList.filter(_.startsWith(prefix)))
      _                      <- ZIO.when(keyList.size != cleanKeyList.size)(warnAboutSpuriousKeys(log, keyList))
      _                      <- log.debug(s"Current key list has ${cleanKeyList.length} elements")
      _                      <- log.debug(s"The first and last elements are ${cleanKeyList.headOption} and ${cleanKeyList.lastOption}")
      previousListOfKeys     <- keysR.getAndSet(cleanKeyList)
      detectedKeyListChanged <- UIO(cleanKeyList != previousListOfKeys)
      _                      <- ZIO.when(detectedKeyListChanged)(log.debug("Detected change in key list") *> keysChangedToken.offer(()))
    } yield EphemeralChange(detectedKeyListChanged)
  }

  protected final def process(keysR: KeysR, keysChangedToken: Queue[Unit], currentState: S, queue: Queue[Chunk[(K, V)]]) = for {
    log       <- logTask
    nextState <- stateFold(keysR, currentState, keysChangedToken)
    _         <- log.debug(s"Next state computed to be $nextState")
    keys      <- keysR.get
    optKey    <- UIO(selectObjectForState(nextState, keys))
    _ <- log.debug(s"Will ask for key $optKey") *> optKey
      .map(getObject(bucketName, _).transduce(transducer).map(value => recordKey(nextState, value) -> value).foreachChunk(queue.offer))
      .getOrElse(ZIO.fail(TamerError(s"File not found with key $optKey for state $nextState"))) // FIXME: relies on nextState.toString
  } yield nextState

  override def iteration(currentState: S, queue: Queue[Chunk[(K, V)]]): RIO[R with Clock with S3, S] = for {
    sourceState <- initialEphemeralState
    token       <- Queue.dropping[Unit](requestedCapacity = 1)
    _ <- updatedSourceState(sourceState, token)
      .scheduleFrom(EphemeralChange.Detected)(Schedule.once ++ fetchSchedule.untilInput(_ == EphemeralChange.Detected))
      .forever
      .fork
    newState <- process(sourceState, token, currentState, queue)
  } yield newState
}

object S3Setup {
  private[this] final val defaultTransducer = ZTransducer.utf8Decode >>> ZTransducer.splitLines

  def apply[R, K: Codec, V: Codec, S: Codec: Hashable](
      bucketName: String,
      prefix: String,
      defaultState: S,
      recordKey: (S, V) => K,
      selectObjectForState: (S, Keys) => Option[String],
      stateFold: (KeysR, S, Queue[Unit]) => UIO[S],
      parallelism: Int = 1,
      minimumIntervalForBucketFetch: Duration,
      maximumIntervalForBucketFetch: Duration,
      transducer: ZTransducer[R, Throwable, Byte, V] = defaultTransducer
  ): S3Setup[R with Clock, K, V, S] = new S3Setup[R with Clock, K, V, S](
    bucketName,
    prefix,
    parallelism,
    minimumIntervalForBucketFetch,
    maximumIntervalForBucketFetch,
    Setup.Serdes[K, V, S],
    defaultState,
    recordKey,
    selectObjectForState,
    stateFold,
    transducer
  ) {}

  private[s3] final def suffixWithoutFileExtension(key: String, prefix: String, formatter: ZonedDateTimeFormatter): String = {
    val dotCountInDate = formatter.format(Instant.EPOCH).count(_ == '.')
    val keyWithoutExtension =
      if (key.count(_ == '.') > dotCountInDate) key.split('.').splitAt(dotCountInDate + 1)._1.mkString(".") else key
    keyWithoutExtension.stripPrefix(prefix)
  }

  private[s3] final def parseInstantFromKey(key: String, prefix: String, formatter: ZonedDateTimeFormatter): Instant =
    Instant.from(formatter.parse(suffixWithoutFileExtension(key, prefix, formatter)))

  private final def getNextInstant(
      keysR: KeysR,
      from: Instant,
      prefix: String,
      formatter: ZonedDateTimeFormatter
  ): ZIO[Any, Nothing, Option[Instant]] = keysR.get.map { keys =>
    val sortedFileDates = keys
      .map(key => parseInstantFromKey(key, prefix, formatter))
      .filter(_ > from)
      .sorted

    sortedFileDates.headOption
  }

  private[s3] final def getNextState(prefix: String, formatter: ZonedDateTimeFormatter)(
      keysR: KeysR,
      from: Instant,
      keysChangedToken: Queue[Unit]
  ): UIO[Instant] = {
    val retryAfterWaitingForKeyListChange =
      keysChangedToken.take *> getNextState(prefix, formatter)(keysR, from, keysChangedToken)
    getNextInstant(keysR, from, prefix, formatter)
      .flatMap {
        case Some(newInstant) if newInstant > from => UIO(newInstant)
        case _                                     => retryAfterWaitingForKeyListChange
      }
  }

  private final def selectObjectForInstant(formatter: ZonedDateTimeFormatter)(from: Instant, keys: Keys): Option[String] =
    keys.find(_.contains(formatter.format(from)))

  final def timed[R, K: Codec, V: Codec](
      bucketName: String,
      filePathPrefix: String,
      from: Instant,
      recordKey: (Instant, V) => K = (l: Instant, _: V) => l,
      transducer: ZTransducer[R, Throwable, Byte, V] = defaultTransducer,
      parallelism: Int = 1,
      dateTimeFormatter: ZonedDateTimeFormatter = ZonedDateTimeFormatter(DateTimeFormatter.ISO_INSTANT, ZoneId.systemDefault()),
      minimumIntervalForBucketFetch: Duration = 5.minutes,
      maximumIntervalForBucketFetch: Duration = 5.minutes
  )(
      implicit ev: Codec[Instant]
  ): S3Setup[R, K, V, Instant] =
    new S3Setup[R, K, V, Instant](
      bucketName,
      filePathPrefix,
      parallelism,
      minimumIntervalForBucketFetch,
      maximumIntervalForBucketFetch,
      Setup.Serdes[K, V, Instant],
      from,
      recordKey,
      selectObjectForInstant(dateTimeFormatter) _,
      getNextState(filePathPrefix, dateTimeFormatter) _,
      transducer
    ) {}
}
