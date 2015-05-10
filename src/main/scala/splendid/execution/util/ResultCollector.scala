package splendid.execution.util

import akka.actor.Stash
import akka.actor.Actor

case object HasNext
case object GetNext

case class Result(value: Any)
case object Done

class ResultCollector extends Actor with Stash {

  val queue = scala.collection.mutable.Queue.empty[Result]

  def collecting: Actor.Receive = {
    case HasNext       => if (queue.isEmpty) stash else sender ! true
    case GetNext       => if (queue.isEmpty) stash else sender ! queue.dequeue
    case value: Result => unstashAll; queue += value
    case Done          => unstashAll; context become serving
  }

  def serving: Actor.Receive = {
    case HasNext => sender ! queue.nonEmpty
    case GetNext => sender ! { if (queue.nonEmpty) queue.dequeue else new NoSuchElementException }
  }

  def receive = collecting
}