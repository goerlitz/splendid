package splendid.execution

import org.openrdf.query.BindingSet
import org.openrdf.query.QueryEvaluationException
import org.openrdf.query.QueryLanguage
import org.openrdf.query.impl.EmptyBindingSet
import org.openrdf.query.parser.QueryParserUtil
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers

import splendid.common.RDF
import splendid.execution.util.SparqlResult
import splendid.execution.util.TestData

class ReactiveEvaluationStrategySpec extends FlatSpecLike with BeforeAndAfterAll with Matchers {

  val EndpointUri = TestData.startSparqlEndpoint()

  override def beforeAll(): Unit = {
    //    testEndpoint.start()
  }

  override def afterAll(): Unit = {
    //    testEndpoint.stop()
  }

  private def eval(query: String, bindings: BindingSet) = {
    val ptq = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, query, null)
    ReactiveEvaluationStrategy().evaluate(ptq.getTupleExpr, bindings)
  }

  "A SERVICE query" should "return 3 predicate bindings" in {

    val query = "SELECT DISTINCT * WHERE { SERVICE <http://localhost:8001/sparql> { [] ?p [] } }"
    val results = eval(query, EmptyBindingSet.getInstance)

    val expectedPredicates = Seq(
      "http://www.perceive.net/schemas/relationship/enemyOf",
      "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
      "http://xmlns.com/foaf/0.1/name")

    results.hasNext() should be(true)
    for (p <- expectedPredicates) results.next() should be(SparqlResult.bindings(("p", RDF.URI(p))))
    results.hasNext() should be(false)
  }

  it should "return one predicate binding for a given BindingSet with predicate" in {

    val query = "SELECT DISTINCT * WHERE { SERVICE <http://localhost:8001/sparql> { [] ?p [] } }"
    val predicateBinding = SparqlResult.bindings(("p", RDF.URI("http://xmlns.com/foaf/0.1/name")))
    val results = eval(query, predicateBinding)

    results.hasNext() should be(true)
    results.next() should be(predicateBinding)
    results.hasNext() should be(false)
  }

  it should "throw an exception if the service URI is an unbound variable" in {
    val query = "SELECT DISTINCT * WHERE { SERVICE ?endpoint { [] ?p [] } }"
    a[QueryEvaluationException] should be thrownBy eval(query, EmptyBindingSet.getInstance)
  }

  it should "use an endpoint URI defined in a BindingSet" in {
    val query = "SELECT DISTINCT ?p WHERE { SERVICE ?endpoint { [] ?p [] } }"
    val endpointBinding = SparqlResult.bindings(
      ("endpoint", RDF.URI("http://localhost:8001/sparql")),
      ("p", RDF.URI("http://xmlns.com/foaf/0.1/name")))
    val results = eval(query, endpointBinding)

    // TODO check if DISTINCT ?p should really return ?endpoint of BindingSet
    results.hasNext() should be(true)
    results.next() should be(endpointBinding)
    results.hasNext() should be(false)
  }

  //  it should "use an endpoint URI defined via BIND" in {
  //    val query = """
  //      | SELECT DISTINCT * WHERE {
  //      |   BIND (<http://localhost:8001/sparql> AS ?endpoint)
  //      |   SERVICE ?endpoint { [] ?p [] }
  //      | }
  //      |""".stripMargin
  //  }
  //  
  //  it should "use an endpoint URI defined via BIND as IRI" in {
  //	  val query = """
  //			  | SELECT DISTINCT * WHERE {
  //			  |   BIND (IRI("http://localhost:8001/sparql") AS ?endpoint)
  //			  |   SERVICE ?endpoint { [] ?p [] }
  //			  | }
  //			  |""".stripMargin
  //  }
  //  
  //  it should "use an endpoint URI defines via VALUES" in {
  //	  val query = """
  //			  | SELECT DISTINCT * WHERE {
  //			  |   VALUES ?endpoint { <http://localhost:8001/sparql> }
  //			  |   SERVICE ?endpoint { [] ?p [] }
  //			  | }
  //			  |""".stripMargin
  //  }
  //  
  //  "A JOIN query" should "return joined results" in {
  //	  val query = "SELECT DISTINCT ?p WHERE {[] ?p ?x. ?x a []} LIMIT 3"
  //  }

}