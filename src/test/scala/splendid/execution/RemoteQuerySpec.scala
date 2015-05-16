package splendid.execution

import java.net.URI

import scala.concurrent.duration.DurationInt

import org.openrdf.query.QueryEvaluationException
import org.openrdf.rio.RDFFormat
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpecLike

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.ImplicitSender
import akka.testkit.TestKit

import splendid.common.RDF
import splendid.execution.util.ResultCollector._
import splendid.execution.util.EndpointClient
import splendid.execution.util.SparqlEndpoint
import splendid.execution.util.TestData
import splendid.execution.util.SparqlResult

/**
 * Test if queries sent to a local SPARQL endpoint return the expected results.
 *
 * Since the [[RemoteQuery]] actor sends all results to its parent actor we need
 * a foster parent to forward all results to the TestKit's actor for checking the expectations.
 */
class RemoteQuerySpec extends TestKit(ActorSystem("RemoteQuerySpec"))
  with FlatSpecLike with BeforeAndAfterAll with ImplicitSender {

  val EndpointUri = TestData.startSparqlEndpoint()

  override def beforeAll(): Unit = {
    //    testEndpoint.start()
  }

  override def afterAll(): Unit = {
    //    testEndpoint.stop()
    system.shutdown()
    system.awaitTermination(10.seconds)
  }

  /**
   * Test helper used to initialize a HTTP client and query a SPARQL endpoint with it.
   */
  private abstract class TestEndpoint(uri: String) {
    def evalQuery(query: String): Unit = system.actorOf(Props(new FosterParent(RemoteQuery.props(uri, query), testActor)))
  }

  "A remote SPARQL query" must "return 3 BindingSets when querying all predicates" in new TestEndpoint(EndpointUri) {

    evalQuery("SELECT DISTINCT ?p WHERE { [] ?p [] } ORDER BY ?p")

    val expectedPredicates = Seq(
      "http://www.perceive.net/schemas/relationship/enemyOf",
      "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
      "http://xmlns.com/foaf/0.1/name")

    val bindings = expectedPredicates.map(p => SparqlResult.bindings(("p", RDF.URI(p))))
    bindings.foreach {
      b => expectMsg(Result(b))
    }
    expectMsg(Done)
  }

  it must "return no result if the triple pattern cannot be matched" in new TestEndpoint(EndpointUri) {
    evalQuery("SELECT ?s WHERE { ?s a <http://example.org/Nothing> }")
    expectMsg(Done)
  }

  it must "return an error for an illegal triple pattern" in new TestEndpoint(EndpointUri) {
    evalQuery("SELECT * WHERE { subject a Nothing }")
    expectMsgPF() { case e: QueryEvaluationException => () }
  }

  it must "return an error for an invalid SPARQL endpoint URI" in new TestEndpoint("http://loclahost:1") {
    evalQuery("SELECT ?s WHERE { ?s a <http://example.org/Nothing> }")
    expectMsgPF() { case e: QueryEvaluationException => () }
  }

}

/**
 * A 'foster parent' actor which receives and forwards messages from the child actor to the test probe.
 */
class FosterParent(childProps: Props, probe: ActorRef) extends Actor {

  val child = context.actorOf(childProps, "child")

  def receive = {
    case msg if sender == child => probe forward msg
    case msg                    => child forward msg
  }
}
