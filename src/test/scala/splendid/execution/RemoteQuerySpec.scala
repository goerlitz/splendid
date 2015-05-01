package splendid.execution

import java.net.URI

import org.openrdf.rio.RDFFormat
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpecLike

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import splendid.Done
import splendid.Result
import splendid.common.RDF
import splendid.execution.util.SparqlEndpoint
import splendid.execution.util.SparqlResult

/**
 * Test if queries sent to a local SPARQL endpoint return the expected results.
 *
 * Since the [[splendid.execution.RemoteQuery]] actor sends all results to its parent actor
 * we need a foster parent to forward all results to the TestKit's actor for checking the expectations.
 */
class RemoteQuerySpec(_system: ActorSystem) extends TestKit(_system)
  with FlatSpecLike with BeforeAndAfterAll with ImplicitSender {
  
  def this() = this(ActorSystem("RemoteQuerySpec"))

  // example RDF data from http://www.w3.org/TR/turtle/
  val DataTTL = """
			| @base <http://example.org/> .
			| @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
			| @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
			| @prefix foaf: <http://xmlns.com/foaf/0.1/> .
			| @prefix rel: <http://www.perceive.net/schemas/relationship/> .
			| 
			| <#green-goblin>
			|     rel:enemyOf <#spiderman> ;
			|     a foaf:Person ;    # in the context of the Marvel universe
			|     foaf:name "Green Goblin" .
			| 
			| <#spiderman>
			|     rel:enemyOf <#green-goblin> ;
			|     a foaf:Person ;
			|     foaf:name "Spiderman", "Человек-паук"@ru .
			""".stripMargin

  val EndpointUri = URI.create("http://localhost:8001/sparql")
  val testEndpoint = SparqlEndpoint(EndpointUri.getPort)

  override def beforeAll(): Unit = {
    testEndpoint.add(DataTTL, "http://example.org/", RDFFormat.TURTLE)
    testEndpoint.start()
  }

  override def afterAll(): Unit = {
    testEndpoint.stop()
    system.shutdown()
  }

  "A remote SPARQL query" must "return the expected results" in {
    
    val query = "SELECT DISTINCT ?p WHERE { [] ?p [] } ORDER BY ?p"
    val expectedPredicates = Seq(
      "http://www.perceive.net/schemas/relationship/enemyOf",
      "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
      "http://xmlns.com/foaf/0.1/name")

    val parentProps = Props(new FosterParent(Props[RemoteQuery], testActor))
    val fosterNode = system.actorOf(parentProps, "foster_parent")

    fosterNode ! SparqlQuery(EndpointUri.toString(), query)

    val bindings = expectedPredicates.map(p => SparqlResult.bindings(("p", RDF.URI(p))))
    bindings.foreach {
      b => expectMsg(Result(b))
    }
    expectMsg(Done)
  }
  
  // TODO add test for empty result set
  
  // TODO add test for failure cases: wrong SPARQL endpoint, wrong SPARQL query

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
