package splendid.execution

import org.openrdf.query.BindingSet

import akka.actor.ActorRef

abstract class OperatorImpl(parent: ActorRef) {
  def handle(sender: ActorRef, bindings: BindingSet)
}