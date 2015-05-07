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

import splendid.Done
import splendid.Result
import splendid.execution.util.SparqlResult

class BindingSetIterationSpec() extends TestKit(ActorSystem("Iteration")) with FlatSpecLike with Matchers {

  "A BindingSetIteration" should "return nothing it there are not results" in {
    val results = sendAndCollectResults(Seq())
    results.size should be(0)
  }

  it should "return all result bindings" in {
    val bindings = SparqlResult.bindings(("o", "bla"))
    val results = sendAndCollectResults(Seq(bindings))
    
    results.size should be(1)
    results.head should equal(bindings)
  }
  
  it should "throw an exception if next() is called on an empty result set" in {
    val it = new BindingSetIteration(Props(new ListActor(Seq())), "", "", EmptyBindingSet.getInstance)
    a [NoSuchElementException] should be thrownBy {
      it.next()
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
    def receive = PartialFunction.empty

    list.foreach { context.parent ! Result(_) }
    context.parent ! Done
  }

}