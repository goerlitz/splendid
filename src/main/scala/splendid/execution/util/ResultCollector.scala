package splendid.execution.util

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import akka.actor.Stash

object ResultCollector {

  case object HasNext
  case object GetNext

  case class Result(value: Any)
  case object Done

  /**
   * Create Props for an actor of this type.
   *
   * @return a [[Props]] which can be further configured.
   */
  def props(): Props = Props(classOf[ResultCollector], None)

  /**
   * Create Props for an actor of this type.
   *
   * @param rootOpProps The Props of the child actor to create.
   * @return a [[Props]] which can be further configured.
   */
  def props(rootOpProps: Props): Props = Props(classOf[ResultCollector], Some(rootOpProps))
}

/**
 * An actor which collects results and serves them to iterators via HasNext and GetNext requests.
 *
 * Since all Splendid actor implementations of query operators send the generated results to their parent actors
 * the result collector must create the root query operator from the given [[Props]] in order to receive results.
 *
 * @param props optional [[Props]] of the root query operator (may be None for testing).
 *
 * @author Olaf Goerlitz
 */
class ResultCollector private (rootOpProps: Option[Props]) extends Actor with Stash with ActorLogging {

  import ResultCollector.{ HasNext, GetNext, Result, Done }

  val queue = scala.collection.mutable.Queue.empty[Result]
  rootOpProps match {
    case Some(props) => context.actorOf(props)
    case None        => // for testing
  }

  def collecting: Actor.Receive = {
    case HasNext       => if (queue.isEmpty) { stash } else { sender ! true }
    case GetNext       => if (queue.isEmpty) { stash } else { sender ! queue.dequeue }
    case Done          => unstashAll; context become serving
    case value: Result => unstashAll; queue += value
    case x             => log.warning(s"unexpected message: $x\n")
  }

  def serving: Actor.Receive = {
    case HasNext => sender ! queue.nonEmpty
    case GetNext => sender ! { if (queue.nonEmpty) queue.dequeue else new NoSuchElementException }
    case x       => log.warning(s"unexpected message: $x\n")
  }

  def receive: Actor.Receive = collecting
}
