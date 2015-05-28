package splendid.execution

import java.util.NoSuchElementException
import scala.collection.mutable.MutableList
import org.openrdf.query.BindingSet
import org.openrdf.query.QueryEvaluationException
import org.openrdf.query.impl.EmptyBindingSet
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.TestKit
import splendid.execution.util.ResultCollector._
import splendid.execution.util.SparqlResult

class BindingSetIterationSpec() extends TestKit(ActorSystem("Iteration")) with FlatSpecLike with Matchers {

  "A BindingSetIteration" should "return nothing if there are no results" in {
    val results = sendAndCollectResults(Seq())
    results.size should be(0)
  }

  it should "return all result bindings" in {
    val bindings = SparqlResult.bindings(("o", "bla"))
    val results = sendAndCollectResults(Seq(bindings))

    results.size should be(1)
    results.head should equal(bindings)
  }

  it should "throw an exception if next() is called on an empty result set after calling hasNext()" in {
    val it = new BindingSetIteration(Props(new ListActor(Seq())), "", "", EmptyBindingSet.getInstance)

    it.hasNext() should be(false)

    a[NoSuchElementException] should be thrownBy {
      it.next()
    }
  }

  it should "throw an exception if next() is called on an empty result set without calling hasNext()" in {
    val it = new BindingSetIteration(Props(new ListActor(Seq())), "", "", EmptyBindingSet.getInstance)

    a[NoSuchElementException] should be thrownBy {
      it.next()
    }
  }

  it should "throw an exception if next() is called after last result has been consumed" in {
    val it = new BindingSetIteration(Props(new ListActor(Seq(EmptyBindingSet.getInstance))), "", "", EmptyBindingSet.getInstance)

    it.hasNext() should be(true)
    it.next() should equal(EmptyBindingSet.getInstance)
    it.hasNext() should be(false)

    a[NoSuchElementException] should be thrownBy {
      it.next()
    }
  }

  it should "throw an exception if remove() is called" in {
    val it = new BindingSetIteration(Props(new ListActor(Seq(EmptyBindingSet.getInstance))), "", "", EmptyBindingSet.getInstance)
    a[UnsupportedOperationException] should be thrownBy {
      it.remove()
    }
  }

  def sendAndCollectResults(resultList: Seq[BindingSet]): Seq[BindingSet] = {
    val it = new BindingSetIteration(Props(new ListActor(resultList)), "", "", EmptyBindingSet.getInstance)

    val received = MutableList[BindingSet]()
    while (it.hasNext()) {
      received += it.next()
    }
    received.toSeq
  }

  private class ListActor(list: Seq[BindingSet]) extends Actor {
    def receive: Actor.Receive = PartialFunction.empty

    list.foreach { context.parent ! Result(_) }
    context.parent ! Done
  }

}
