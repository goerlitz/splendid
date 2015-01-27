package splendid.execution

import scala.collection.JavaConversions.seqAsJavaList
import scala.concurrent.duration.DurationInt

import org.openrdf.model.impl.ValueFactoryImpl
import org.openrdf.query.BindingSet
import org.openrdf.query.impl.EmptyBindingSet
import org.openrdf.query.impl.ListBindingSet
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Finders
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit

class ParallelHashJoinSpec(_system: ActorSystem) extends TestKit(_system)
  with ImplicitSender
  with Matchers
  with FlatSpecLike
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("ParallelHashJoinSpec"))

  override def afterAll: Unit = {
    system.shutdown()
    system.awaitTermination(10 seconds)
  }

  /**
   * Defines fixtures for every test.
   */
  trait ActorSetup {
    val leftSource = TestActorRef(Props[OperatorNode])
    val rightSource = TestActorRef(Props[OperatorNode])
    val collector = TestActorRef[ResultSet](Props[ResultSet])

    val fac = new ValueFactoryImpl

    def bindings(tuples: (String, Any)*): BindingSet = {
      val (names, values) = tuples.unzip
      new ListBindingSet(names.toList, values.map { x => fac.createLiteral(x.toString) })
    }
  }
  

  "A ParallelHashJoin" should "throw IllegalArgumentException if BindingSet is empty" in new ActorSetup {

    val join = ParallelHashJoin(Set("x"), leftSource, rightSource, collector)

    intercept[IllegalArgumentException] {
      join.handle(leftSource, EmptyBindingSet.getInstance)
    }
  }

  it should "throw IllegalArgumentException if BindingSet has wrong join variables" in new ActorSetup {

    val join = ParallelHashJoin(Set("x"), leftSource, rightSource, collector)

    intercept[IllegalArgumentException] {
      join.handle(leftSource, bindings("y" -> 1))
    }
  }

  it should "return correct BindingSets for one join variable" in new ActorSetup {

    val join = ParallelHashJoin(Set("x"), leftSource, rightSource, collector)

    val target = collector.underlyingActor

    assertResult(0, "entries in result set")(target.results.size)
    
    join.handle(leftSource, bindings("x" -> 1, "z" -> 3))
    join.handle(leftSource, bindings("x" -> 1, "z" -> 4))
    join.handle(leftSource, bindings("x" -> 2, "z" -> 3))
    join.handle(rightSource, bindings("x" -> 1, "y" -> 2))
    
    assertResult(2, "entries in result set")(target.results.size)
    assert(target.results.contains(bindings("x" -> 1, "y" -> 2, "z" -> 3)))
    assert(target.results.contains(bindings("x" -> 1, "y" -> 2, "z" -> 4)))
  }
  
  it should "return correct BindingSets for two join variables" in new ActorSetup {

    val join = ParallelHashJoin(Set("x", "y"), leftSource, rightSource, collector)

    val results = collector.underlyingActor
    
    assertResult(0, "entries in result set")(results.results.size)
    
    join.handle(leftSource, bindings("x" -> 1, "y" -> 1, "z" -> 3))
    join.handle(leftSource, bindings("x" -> 1, "y" -> 2, "z" -> 4))
    join.handle(rightSource, bindings("x" -> 1, "y" -> 2, "a" -> 0))
    
    assertResult(1, "entries in result set")(results.results.size)
    assert(results.results.contains(bindings("a" -> 0, "x" -> 1, "y" -> 2, "z" -> 4)))
  }
  
  it should "return the cross product for no join variables" in new ActorSetup {

    val join = ParallelHashJoin(Set(), leftSource, rightSource, collector)

    val target = collector.underlyingActor
    
    assertResult(0, "entries in result set")(target.results.size)
    
    join.handle(leftSource, bindings("x" -> 1, "y" -> 1))
    join.handle(leftSource, bindings("x" -> 1))
    join.handle(rightSource, bindings("a" -> 1, "b" -> 2))
    join.handle(rightSource, bindings("a" -> 1))
    
    assertResult(4, "entries in result set")(target.results.size)
    assert(target.results.contains(bindings("a" -> 1, "b" -> 2, "x" -> 1, "y" -> 1)))
    assert(target.results.contains(bindings("a" -> 1, "b" -> 2, "x" -> 1)))
    assert(target.results.contains(bindings("a" -> 1, "x" -> 1, "y" -> 1)))
    assert(target.results.contains(bindings("a" -> 1, "x" -> 1)))
  }
}

/**
 * A result collector.
 */
class ResultSet extends Actor {

  val results = scala.collection.mutable.Set[BindingSet]()

  def receive = {
    case TupleResult(bindings) => {
      results.add(bindings)
    }
    case x => Console.println(s"unknown event ${x.getClass()}")
  }
}