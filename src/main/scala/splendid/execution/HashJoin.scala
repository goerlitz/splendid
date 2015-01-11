package splendid.execution

import org.openrdf.model.Value
import org.openrdf.query.BindingSet
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import splendid.Result
import splendid.Setup
import org.openrdf.query.algebra.evaluation.QueryBindingSet

class HashJoin(joinVars: Seq[String], left: ActorRef, right: ActorRef) extends Actor with ActorLogging {

  import context._

  val leftBindings = collection.mutable.Map[Seq[Value], BindingSet]()
  val rightBindings = collection.mutable.Map[Seq[Value], BindingSet]()

  def receive = {
    case Setup(parent) => {
      left ! Setup(self)
      right ! Setup(self)
      become(initialized(parent))
      println(s"Initialized actor with parent: $parent")
    }
    case _ => log.warning("actor has not been initialized $this")
  }

  def initialized(parent: ActorRef): Receive = {
    case Result(bs) => sender match {
      case `left` => {
        log.info(s"got result $bs from left child (joinVars is $joinVars)")
        val values = joinVars.map { x => bs.getBinding(x).getValue }
        val result = handle(values, rightBindings)
        result match {
          case Some(resultBS) => {
            resultBS.addAll(bs)
//            log.info(s"found matching right binding $resultBS")
            parent ! Result(resultBS)
          }
          case None =>
        }
        // store bindings
        leftBindings += values -> bs
      }
      case `right` => {
        log.info(s"got result $bs from right child (joinVars is $joinVars)")
        val values = joinVars.map { x => bs.getBinding(x).getValue }
        val result = handle(values, leftBindings)
        result match {
          case Some(resultBS) => {
            resultBS.addAll(bs)
//            log.info(s"found matching left binding $resultBS")
            parent ! Result(resultBS)
          }
          case None =>
        }
        rightBindings += values -> bs
      }
      case _ => log.warning("got result $bs from unknown sender $sender")
    }
    case _ => log.warning("got unknown message")
  }

  def handle(values: Seq[Value], other: collection.mutable.Map[Seq[Value], BindingSet]) = {
    for (bla <- other.get(values)) yield new QueryBindingSet(bla)
  }
}