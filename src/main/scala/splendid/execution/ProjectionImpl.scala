package splendid.execution

import scala.collection.JavaConversions._

import org.openrdf.query.BindingSet
import org.openrdf.query.impl.ListBindingSet

import akka.actor.ActorRef

class ProjectionImpl(projElements: Set[String], child: ActorRef, parent: ActorRef) extends OperatorImpl(parent) {

  override def handle(sender: ActorRef, bindings: BindingSet): Unit = {
    val inter = (projElements intersect bindings.getBindingNames).toSeq
    if (!inter.isEmpty) {
      val values = inter map { x => bindings.getValue(x) }
      parent ! TupleResult(new ListBindingSet(inter, values))
    }
  }
}