package splendid.execution.util

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import org.openrdf.query.BindingSet
import org.openrdf.query.QueryEvaluationException

import ResultCollector.GetNext
import ResultCollector.HasNext
import ResultCollector.Result
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import info.aduna.iteration.CloseableIteration

/**
 * Implementation of a [[org.openrdf.query.BindingSet]] iterator based on an underlying result collector.
 * 
 * @author Olaf Goerlitz
 */
class ResultStreamIteration(resultCollector: ActorRef) extends CloseableIteration[BindingSet, QueryEvaluationException] {

  implicit val timeout: Timeout = Timeout(10 seconds)
  
  import ResultCollector._

  override def hasNext(): Boolean = Await.result(resultCollector ? HasNext, Duration.Inf) match {
    case b: Boolean => b
    case x          => throw new IllegalStateException(s"illegal response in hasNext(): $x")
  }

  override def next(): BindingSet = Await.result(resultCollector ? GetNext, Duration.Inf) match {
    case Result(value: BindingSet) => value
    case e: Throwable              => throw e
    case x                         => throw new IllegalStateException(s"illegal response in next(): $x")
  }

  override def remove(): Unit = throw new UnsupportedOperationException

  override def close(): Unit = {
  }
}
