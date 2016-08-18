package splendid.execution

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration._

import org.openrdf.model.impl.SimpleValueFactory
import org.openrdf.query.BindingSet
import org.openrdf.query.impl.ListBindingSet
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers

import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit

class ProjectionSpec(_system: ActorSystem) extends TestKit(_system)
    with ImplicitSender
    with Matchers
    with FlatSpecLike
    with BeforeAndAfterAll {

  def this() = this(ActorSystem("ParallelHashJoinSpec"))

  override def afterAll: Unit = {
    system.terminate()
    Await.result(system.whenTerminated, 10.seconds)
  }

  trait ActorSetup {
    val source = TestActorRef(Props[OperatorNode])
    val collector = TestActorRef[ResultSet](Props[ResultSet])

    val fac = SimpleValueFactory.getInstance

    def bindings(tuples: (String, Any)*): BindingSet = {
      val (names, values) = tuples.unzip
      new ListBindingSet(names.toList, values.map { x => fac.createLiteral(x.toString) })
    }
  }

  "A projection" should "remove bindings" in new ActorSetup {

    val projection = new ProjectionImpl(Set("x"), source, collector)

    val target = collector.underlyingActor

    assertResult(0, "entries in result set")(target.results.size)

    projection.handle(source, bindings("x" -> 1, "y" -> 2))

    assertResult(1, "entries in result set")(target.results.size)
    assert(target.results.contains(bindings("x" -> 1)))

    projection.handle(source, bindings("a" -> 1, "b" -> 2))

    assertResult(1, "entries in result set")(target.results.size)
  }

  it should "not produce empty bindings" in new ActorSetup {
    val projection = new ProjectionImpl(Set("x"), source, collector)

    val target = collector.underlyingActor

    assertResult(0, "entries in result set")(target.results.size)

    projection.handle(source, bindings("a" -> 1, "b" -> 2))

    assertResult(0, "entries in result set")(target.results.size)
  }

}
