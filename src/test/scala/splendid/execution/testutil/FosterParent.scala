package splendid.execution.testutil

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props

object FosterParent {
  def props(childProps: Props, probe: ActorRef): Props = Props(new FosterParent(childProps, probe))
}

/**
 * A 'foster parent' actor which receives and forwards messages from the child actor to the test probe.
 */
class FosterParent private (childProps: Props, probe: ActorRef) extends Actor {

  val child = context.actorOf(childProps, "child")

  def receive: Actor.Receive = {
    case msg if sender == child => probe forward msg
    case msg                    => child forward msg
  }
}
