package splendid.execution

import scala.concurrent.duration.DurationInt

import org.openrdf.query.BindingSet
import org.openrdf.query.QueryEvaluationException
import org.openrdf.query.impl.EmptyBindingSet
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpecLike

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import splendid.common.RDF
import splendid.execution.util.ResultCollector.Done
import splendid.execution.util.ResultCollector.Result
import splendid.execution.util.SparqlResult
import splendid.execution.util.TestData

/**
 * Test if queries sent to a local SPARQL endpoint return the expected results.
 *
 * Since the [[RemoteQuery]] actor sends all results to its parent actor we need
 * a foster parent to forward all results to the TestKit's actor for checking the expectations.
 */
class RemoteQuerySpec extends TestKit(ActorSystem("RemoteQuerySpec"))
  with FlatSpecLike with BeforeAndAfterAll with ImplicitSender {

  val EndpointUri = TestData.startSparqlEndpoint()

  val EmptyBindings = EmptyBindingSet.getInstance

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
    def evalQuery(query: String, bindings: BindingSet): Unit = system.actorOf(FosterParent.props(RemoteQuery.props(uri, query, bindings)))
  }

  "A remote SPARQL query" must "return 3 BindingSets when querying all predicates" in new TestEndpoint(EndpointUri) {

    evalQuery("SELECT DISTINCT ?p WHERE { [] ?p [] } ORDER BY ?p", EmptyBindings)

    val bindings = TestData.AllPredicates.map(p => SparqlResult.bindings(("p", RDF.URI(p))))
    for (bs <- bindings) expectMsg(Result(bs))
    expectMsg(Done)
  }

  // TODO test queries with multiple free variables

  it must "return no result if the triple pattern cannot be matched" in new TestEndpoint(EndpointUri) {
    evalQuery("SELECT ?s WHERE { ?s a <http://example.org/Nothing> }", EmptyBindings)
    expectMsg(Done)
  }

  it must "return an error for an illegal triple pattern" in new TestEndpoint(EndpointUri) {
    evalQuery("SELECT * WHERE { subject a Nothing }", EmptyBindings)
    expectMsgPF() { case e: QueryEvaluationException => () }
  }

  it must "return an error for an invalid SPARQL endpoint URI" in new TestEndpoint("http://loclahost:1") {
    evalQuery("SELECT ?s WHERE { ?s a <http://example.org/Nothing> }", EmptyBindings)
    expectMsgPF() { case e: QueryEvaluationException => () }
  }

  it must "return 1 BindingSets when querying all predicates and a binding for ?p is given" in new TestEndpoint(EndpointUri) {

    val predicateBinding = SparqlResult.bindings(("p", RDF.URI("http://xmlns.com/foaf/0.1/name")))
    evalQuery("SELECT DISTINCT ?p WHERE { [] ?p [] } ORDER BY ?p", predicateBinding)

    expectMsg(Result(predicateBinding))
    expectMsg(Done)
  }

  it must "return no result if the triple pattern cannot be matched and a binding is given" in new TestEndpoint(EndpointUri) {
    val subjectBinding = SparqlResult.bindings(("s", RDF.URI("http://example.org/#spiderman")))
    evalQuery("SELECT ?s WHERE { ?s a <http://example.org/Nothing> }", subjectBinding)
    expectMsg(Done)
  }

  /**
   * A 'foster parent' actor which receives and forwards messages from the child actor to the test probe.
   */
  protected class FosterParent private (childProps: Props, probe: ActorRef) extends Actor {

    val child = context.actorOf(childProps, "child")

    def receive: Actor.Receive = {
      case msg if sender == child => probe forward msg
      case msg                    => child forward msg
    }
  }

  protected object FosterParent {
    def props(childProps: Props): Props = Props(new FosterParent(childProps, testActor))
  }
}

