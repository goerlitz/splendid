package splendid.execution

import org.openrdf.model.Value
import org.openrdf.query.BindingSet
import org.openrdf.query.algebra.evaluation.QueryBindingSet

import akka.actor.ActorRef
import akka.actor.actorRef2Scala

import scala.collection.JavaConversions._

object ParallelHashJoin {
  def apply(joinVars: Set[String], left: ActorRef, right: ActorRef, parent: ActorRef) = new ParallelHashJoin(joinVars, left, right, parent);
}

class ParallelHashJoin(joinVars: Set[String], left: ActorRef, right: ActorRef, parent: ActorRef) extends OperatorImpl(parent) {

  val leftBindings = collection.mutable.Map[Map[String, Value], List[BindingSet]]()
  val rightBindings = collection.mutable.Map[Map[String, Value], List[BindingSet]]()

  override def handle(sender: ActorRef, bindings: BindingSet): Unit = {

    if (!bindings.getBindingNames.containsAll(joinVars)) {
      throw new IllegalArgumentException(s"wrong join vars. expected $joinVars but found ${bindings.getBindingNames}")
    }

    // get variable bindings of join variables
    val joinVals = { for (b <- bindings if joinVars.contains(b.getName)) yield (b.getName -> b.getValue) }.toMap

    // determine map references depending on sender
    val (ownMap, otherMap) = sender match {
      case `left`  => (leftBindings, rightBindings)
      case `right` => (rightBindings, leftBindings)
      case _       => throw new IllegalArgumentException("IllegalSender")
    }

    // find matching tuples from other result set
    otherMap.get(joinVals) match {
      case Some(otherBindings) => for (otherResult <- otherBindings) parent ! TupleResult(join(bindings, otherResult))
      case None                => // no results
    }

    // add new result tuple
    ownMap.get(joinVals) match {
      case Some(ownBindings) => ownMap.put(joinVals, bindings :: ownBindings)
      case None              => ownMap.put(joinVals, List(bindings))
    }
  }

  def join(firstBinding: BindingSet, secondBinding: BindingSet): BindingSet = {
    val result = new QueryBindingSet(firstBinding)
    result.addAll(secondBinding)
    return result
  }
}