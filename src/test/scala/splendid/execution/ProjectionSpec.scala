package splendid.execution

import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.BeforeAndAfterAll
import akka.actor.ActorSystem
import org.scalatest.Matchers
import org.scalatest.FlatSpecLike
import scala.concurrent.duration.DurationInt
import akka.actor.Props
import akka.testkit.TestActorRef
import org.openrdf.query.BindingSet
import org.openrdf.query.impl.ListBindingSet
import org.openrdf.model.impl.ValueFactoryImpl

import scala.collection.JavaConversions._

class ProjectionSpec(_system: ActorSystem) extends TestKit(_system)
    with ImplicitSender
    with Matchers
    with FlatSpecLike
    with BeforeAndAfterAll {

  def this() = this(ActorSystem("ParallelHashJoinSpec"))

  override def afterAll: Unit = {
    system.shutdown()
    system.awaitTermination(10.seconds)
  }

  trait ActorSetup {
    val source = TestActorRef(Props[OperatorNode])
    val collector = TestActorRef[ResultSet](Props[ResultSet])

    val fac = new ValueFactoryImpl

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
