package splendid.execution

import org.openrdf.query.BindingSet
import org.openrdf.query.algebra.Join

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import splendid.execution.ReactiveEvaluationStrategy.PropsFun
import splendid.execution.util.ResultCollector.Done
import splendid.execution.util.ResultCollector.Result

object NestedLoopJoin {
  def props(join: Join, bindings: BindingSet, getProps: PropsFun): Props = Props(classOf[NestedLoopJoin], join, bindings, getProps)
}

/**
 * Reactive implementation of a Nested Loop Join.
 *
 * @param join The join expression.
 * @param bindings An optional BindingSet.
 * @param propsFun A partial function for generating actor properties.
 *
 * @author Olaf Goerlitz
 */
class NestedLoopJoin private (join: Join, bindings: BindingSet, getProps: PropsFun) extends Actor with ActorLogging {

  val leftChild = context.actorOf(getProps(join.getLeftArg, bindings))

  /**
   * Receive function which tracks the state of the right and left child actors.
   * @param leftDone true if the left child actor has produced all results.
   * @param rightRunning number of right child actors which are still executing the right query argument.
   * @return a receive function
   */
  def waitingForResults(leftDone: Boolean, rightRunning: Int): Actor.Receive = {

    case Result(bs: BindingSet) if sender == leftChild =>
      // create child actor for right argument with given bindings and increase number of running actors
      context.actorOf(getProps(join.getRightArg, bs))
      context become waitingForResults(leftDone, rightRunning + 1)

    case Done if sender == leftChild =>
      // change done state for left child actor
      context become waitingForResults(true, rightRunning)

    case result: Result =>
      // forward results from right child actor
      context.parent ! result

    case Done if (leftDone && (rightRunning == 1)) =>
      // last done (rightRunning would be 0 afterwards)
      context.parent ! Done
      context.system.stop(self)

    case Done => context become waitingForResults(leftDone, rightRunning - 1)

    case x    => log.warning(s"unknown message $x")
  }

  def receive: Actor.Receive = waitingForResults(false, 0)
}
