package splendid.execution

import org.openrdf.model.Value
import org.openrdf.query.BindingSet
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import splendid.execution.util.ResultCollector._
import splendid.Setup
import org.openrdf.query.algebra.evaluation.QueryBindingSet

/**
 * A simple symmetric hash join implementation.
 * 
 * @author Olaf Goerlitz
 */
class HashJoin(joinVars: Seq[String], left: ActorRef, right: ActorRef) extends Actor with ActorLogging {

  import context._

  val leftBindings = collection.mutable.Map[Seq[Value], List[BindingSet]]()
  val rightBindings = collection.mutable.Map[Seq[Value], List[BindingSet]]()

  def receive = {
    // initialize with parent reference
    case Setup(parent) => {
      left ! Setup(self)
      right ! Setup(self)
      become(initialized(parent))
      log.info(s"Initialized actor with parent: $parent")
    }
    case _ => throw new IllegalStateException(s"actor has not been initialized yet $this")
      //log.warning("actor has not been initialized $this")
  }

  def initialized(parent: ActorRef): Receive = {
    // process results depending on sender
    case Result(newResult: BindingSet) => {

      // get variable bindings of join variables
      val joinValues = joinVars.map { x => newResult.getBinding(x).getValue }

      // determine map references depending on sender
      val (ownMap, otherMap) = sender match {
        case `left`  => (leftBindings, rightBindings)
        case `right` => (rightBindings, leftBindings)
        case _ => throw new IllegalArgumentException("IllegalSender")
      }

      // find matching tuples from other result set
      otherMap.get(joinValues) match {
        case Some(bindings) => for (otherResult <- bindings) parent ! Result(join(newResult, otherResult))
        case None           => // no results
      }

      // save new result tuple
      ownMap.get(joinValues) match {
        case Some(bindings) => ownMap.put(joinValues, newResult :: bindings)
        case None           => ownMap.put(joinValues, List(newResult))
      }
    }
    case _ => log.warning("got unknown message")
  }

  def join(firstBinding: BindingSet, secondBinding: BindingSet): BindingSet = {
    val result = new QueryBindingSet(firstBinding)
    result.addAll(secondBinding)
    return result
  }

}