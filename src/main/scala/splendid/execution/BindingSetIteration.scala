package splendid.execution

import java.util.concurrent.ArrayBlockingQueue

import org.openrdf.query.BindingSet
import org.openrdf.query.QueryEvaluationException

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props

import info.aduna.iteration.CloseableIteration

import splendid.Done
import splendid.Result

/**
 * Bridge between the Actors push-based reactive result generation and the pull-based result processing in Sesame's Iterations.
 * 
 * NOT THREAD-SAFE!
 */
class BindingSetIteration(props: Props, uri: String, query: String, bindings: BindingSet) extends CloseableIteration[BindingSet, QueryEvaluationException] {

  var done = false
  var peek: Option[BindingSet] = None
  val resultQueue = new ArrayBlockingQueue[Option[BindingSet]](100)

  val system = ActorSystem("my_operators")
  val rootNode = system.actorOf(Props(new ResultCollector(props)), "root")

  rootNode ! SparqlQuery(uri, query)

  override def hasNext() = !done && (peek.nonEmpty || (resultQueue.take() match {
    case Some(bs) => {
      peek = Some(bs); true
    }
    case None => { // end of queue
      done = true
      false
    }
  }))

  @throws(classOf[QueryEvaluationException])
  override def next(): BindingSet = if (done) {
    throw new NoSuchElementException
  } else {
    peek match {
      case Some(bs) => {
        peek = None
        bs
      }
      case None => resultQueue.take() getOrElse { // end of queue 
        done = true
        throw new NoSuchElementException
      }
    }
  }

  override def remove() = throw new UnsupportedOperationException()

  // TODO: stop actors
  override def close() = throw new UnsupportedOperationException()

  class ResultCollector(props: Props) extends Actor with ActorLogging {

    val child = context.actorOf(props)

    def receive = {
      case Result(bindings) => resultQueue.put(Some(bindings))
      case Done => {
        resultQueue.put(None)
        context.system.shutdown();
      }
      case msg if sender != child => child forward msg
      case msg                    => log.warning("unknown message $msg")
    }
  }
}
