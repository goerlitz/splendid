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

  val left = TestActorRef(Props[OperatorNode], "left")
  val right = TestActorRef(Props[OperatorNode], "right")
  val listener = TestActorRef[ResultSet](Props[ResultSet], "listener")

  "A ParallelHashJoin" should "throw IllegalArgumentException if BindingSet is empty" in {

    val join = ParallelHashJoin(Set("x"), left, right, listener)

    intercept[IllegalArgumentException] {
      join.handle(left, EmptyBindingSet.getInstance)
    }
  }

  it should "throw IllegalArgumentException if BindingSet has wrong join variables" in {

    val join = ParallelHashJoin(Set("x"), left, right, listener)

    intercept[IllegalArgumentException] {
      join.handle(left, Binder.literal("y" -> 1))
    }
  }

  it should "return correct BindingSets for one join variable" in {

    val join = ParallelHashJoin(Set("x"), left, right, listener)

    val results = listener.underlyingActor

    results.results.clear()
    assert(results.results.size == 0)
    
    join.handle(left, Binder.literal("x" -> 1, "z" -> 3))
    join.handle(left, Binder.literal("x" -> 1, "z" -> 4))
    join.handle(left, Binder.literal("x" -> 2, "z" -> 3))
    join.handle(right, Binder.literal("x" -> 1, "y" -> 2))
    
    assert(results.results.size == 2)
    assert(results.results.contains(Binder.literal("x" -> 1, "y" -> 2, "z" -> 3)))
    assert(results.results.contains(Binder.literal("x" -> 1, "y" -> 2, "z" -> 4)))
  }
  
  it should "return correct BindingSets for two join variables" in {

    val join = ParallelHashJoin(Set("x", "y"), left, right, listener)

    val results = listener.underlyingActor
    
    results.results.clear()
    assert(results.results.size == 0)
    
    join.handle(left, Binder.literal("x" -> 1, "y" -> 1, "z" -> 3))
    join.handle(left, Binder.literal("x" -> 1, "y" -> 2, "z" -> 4))
    join.handle(right, Binder.literal("x" -> 1, "y" -> 2, "a" -> 0))
    
    assert(results.results.size == 1)
    assert(results.results.contains(Binder.literal("a" -> 0, "x" -> 1, "y" -> 2, "z" -> 4)))
  }
  
  it should "return the cross product for no join variables" in {

    val join = ParallelHashJoin(Set(), left, right, listener)

    val results = listener.underlyingActor
    
    results.results.clear()
    assert(results.results.size == 0)
    
    join.handle(left, Binder.literal("x" -> 1, "y" -> 1))
    join.handle(left, Binder.literal("x" -> 1))
    join.handle(right, Binder.literal("a" -> 1, "b" -> 2))
    join.handle(right, Binder.literal("a" -> 1))
    
    assert(results.results.size == 4)
    assert(results.results.contains(Binder.literal("a" -> 1, "b" -> 2, "x" -> 1, "y" -> 1)))
    assert(results.results.contains(Binder.literal("a" -> 1, "b" -> 2, "x" -> 1)))
    assert(results.results.contains(Binder.literal("a" -> 1, "x" -> 1, "y" -> 1)))
    assert(results.results.contains(Binder.literal("a" -> 1, "x" -> 1)))
  }
}

object Binder {

  def fac = new ValueFactoryImpl

  def literal(tuples: (String, Any)*): BindingSet = {
    val (names, values) = tuples.unzip
    new ListBindingSet(names.toList, values.map { x => fac.createLiteral(x.toString) })
  }
}

class ResultSet extends Actor {
  
  val results = scala.collection.mutable.Set[BindingSet]()
  
  def receive = {
    case TupleResult(bindings) => {
      results.add(bindings)
    }
    case x => Console.println(s"unknown event ${x.getClass()}")
  }
}