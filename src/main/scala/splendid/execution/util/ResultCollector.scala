package splendid.execution.util

import akka.actor.Actor
import akka.actor.Stash
import akka.actor.Props
import akka.actor.Status.Failure

case object HasNext
case object GetNext

case class Result(value: Any)
case object Done

/**
 * An actor which collects results from a child actor and serves them to iterators via HasNext and GetNext requests.
 *
 * @param props optional child actor configuration
 *
 * @author Olaf Goerlitz
 */
class ResultCollector(props: Option[Props]) extends Actor with Stash {

  val queue = scala.collection.mutable.Queue.empty[Result]
  props match { case Some(props) => context.actorOf(props) case None => }

  def collecting: Actor.Receive = {
    case HasNext       => if (queue.isEmpty) stash else sender ! true
    case GetNext       => if (queue.isEmpty) stash else sender ! queue.dequeue
    case Done          => unstashAll; context become serving
    case value: Result => unstashAll; queue += value
    case x             => sender ! Failure(new UnsupportedOperationException(s"unknown message $x"))
  }

  def serving: Actor.Receive = {
    case HasNext => sender ! queue.nonEmpty
    case GetNext => sender ! { if (queue.nonEmpty) queue.dequeue else new NoSuchElementException }
    case x       => sender ! Failure(new UnsupportedOperationException(s"unknown message $x"))
  }

  def receive = collecting
}

object ResultCollector {
  def apply() = new ResultCollector(None)
  def apply(props: Props) = new ResultCollector(Some(props))
}