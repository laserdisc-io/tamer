package tamer

//import io.prometheus.client.{CollectorRegistry, Counter}
import zio._
//Notes : Maybe try Mixin with Setup abstract class ??

trait StateMonitor[-R, -State, Result] {
  def notifyCompletion(result: Either[Option[Throwable], (Option[NonEmptyChunk[Result]], State)]): URIO[R, Unit]
}
//abstract class SetupWithMetrics[-R ,K, V, S] extends Setup[R ,K, V, S] with StateMonitor[S,(K,V)]
object StateMonitor {
  def noOp[R, S, T]: StateMonitor[R, S, T] = new StateMonitor[R, S, T] {
    override def notifyCompletion(result: Either[Option[Throwable], (Option[NonEmptyChunk[T]], S)]): URIO[R, Unit] = Task.unit
  }

  def countingStateMonitor[R, S, T](result: Either[Option[Throwable], (Option[NonEmptyChunk[T]], S)]) = new StateMonitor[R, S, T] {

    // TODO registry
    //    val counterOk: Counter    = Counter.build().name("state-success-counter").help("???").create()
    //    val counterNotOk: Counter = Counter.build().name("state-failure-counter").help("???").create()
    //    val counterRecords: Counter = Counter.build().name("state-failure-counter").help("???").create()
    override def notifyCompletion(result: Either[Option[Throwable], (Option[NonEmptyChunk[T]], S)]) = {
      result match {
        case Left(value) => // increaseError(errorCategore(value))
          println("FAILUUUUUUUUUUUUUUUUUUUUUUUUUURE CNT----------------------------------------------------")
        case Right(value) =>
          println("SUCCESSSSSSSSSSSSSSSSSSSSSSSSSSSSSss CNT----------------------------------------------------")
        // increaseOk *> increaseRecords(value.size)
      }
      URIO.unit
    }
  }
}

//object Prometheus {
//  // import io.prometheus.client.Collector
//  // import io.prometheus.client.SimpleCollector
//  import io.prometheus.client.{Counter, Gauge}
//  val requests = Counter
//    .build()
//    .name("requests_total")
//    .help("Total requests.")
//    .register();
//  val inprogressRequests = Gauge
//    .build()
//    .name("inprogress_requests")
//    .help("Inprogress requests.")
//    .register();
//
//  requests.inc()
//
//}
