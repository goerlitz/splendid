package splendid.execution

import org.openrdf.query.BindingSet
import org.openrdf.query.QueryLanguage
import org.openrdf.query.algebra.Join
import org.openrdf.query.algebra.Service
import org.openrdf.query.algebra.TupleExpr
import org.openrdf.query.algebra.Union
import org.openrdf.query.impl.EmptyBindingSet
import org.openrdf.query.parser.QueryParserUtil
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Status
import akka.actor.actorRef2Scala
import splendid.Result
import scala.collection.JavaConversions._
import akka.actor.ActorContext

case class TupleExprOp(expr: TupleExpr, bindings: BindingSet)
case class TupleResult(bindings: BindingSet)

class OperatorNode extends Actor with ActorLogging {

  import context._

  def receive = {
    case TupleExprOp(expr, bindings) => expr match {
      case join: Join => handle(join, bindings)
      case service: Service => {

      }

      case union: Union => {

      }

      case x => log.warning(s"unknown expression " + x.getClass)
    }
    case err : Status.Failure => 
    case _ => stop(self); sender ! Status.Failure(new UnsupportedOperationException)
  }

  def eval(operatorImpl: OperatorImpl): Receive = {
    case TupleResult(bindings) => operatorImpl.handle(sender, bindings)
    case _                     => stop(self); sender ! Status.Failure(new UnsupportedOperationException)
  }

  def handle(join: Join, bindings: BindingSet): Unit = {

    val joinVars = join.getLeftArg.getBindingNames.toSet intersect join.getRightArg.getBindingNames
    val left = actorOf(Props[OperatorNode], "left")
    val right = actorOf(Props[OperatorNode], "right")

    become(eval(ParallelHashJoin(joinVars, left, right, parent)))
    
    left ! TupleExprOp(join.getLeftArg, bindings)
    right ! TupleExprOp(join.getRightArg, bindings)
  }

  def joinOp(left: ActorRef, right: ActorRef): Receive = {
    case TupleResult(bindings) => log.warning(s"got binding $bindings")
    case _                     => sender ! Status.Failure
  }
}
