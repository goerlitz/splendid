package splendid.execution.util

import java.net.URI

import org.openrdf.rio.RDFFormat

object TestData {

  // example RDF data from http://www.w3.org/TR/turtle/
  val TTL = """
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

  val AllPredicates = Seq(
    "http://www.perceive.net/schemas/relationship/enemyOf",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
    "http://xmlns.com/foaf/0.1/name")

  // the LinkedDataServer allows only one instance (in all tests)
  private var sparqlEndpoint: Option[SparqlEndpoint] = None

  private val EndpointUri = "http://localhost:8001/sparql"

  // TODO improve (not thread-safe)
  def startSparqlEndpoint(): String = {
    sparqlEndpoint match {
      case None =>
        val testEndpoint = SparqlEndpoint(URI.create(EndpointUri).getPort)
        testEndpoint.add(TTL, "http://example.org/", RDFFormat.TURTLE)
        testEndpoint.start()
        sparqlEndpoint = Some(testEndpoint)
      case Some(_) =>
    }
    EndpointUri
  }
}