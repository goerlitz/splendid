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
import splendid.execution.ReactiveEvaluationStrategy.BindingsIteration
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

  private def expectAllPredicates(results: BindingsIteration): Unit =
    expectBindings(results, TestData.AllPredicates.map { x => SparqlResult.bindings(("p", RDF.URI(x))) })

  private def expectBindings(results: BindingsIteration, bindings: Seq[BindingSet]): Unit = {
    results.hasNext() should be(true)
    for (bs <- bindings) results.next() should be(bs)
    results.hasNext() should be(false)
  }

  "A SERVICE query" should "return 3 predicate bindings" in {

    val query = "SELECT DISTINCT * WHERE { SERVICE <http://localhost:8001/sparql> { [] ?p [] } }"
    val results = eval(query, EmptyBindingSet.getInstance)

    expectAllPredicates(results)
  }

  it should "return one predicate binding for a given BindingSet with predicate" in {

    val query = "SELECT DISTINCT * WHERE { SERVICE <http://localhost:8001/sparql> { [] ?p [] } }"
    val predicateBinding = SparqlResult.bindings(("p", RDF.URI("http://xmlns.com/foaf/0.1/name")))
    val results = eval(query, predicateBinding)

    expectBindings(results, Seq(predicateBinding))
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
    expectBindings(results, Seq(endpointBinding))
  }

  "A Join query" should "return all predicate bindings" in {
    val query = """
          | SELECT DISTINCT ?p WHERE {
          |   SERVICE <http://localhost:8001/sparql> { ?s a ?type }
          |   SERVICE <http://localhost:8001/sparql> { ?s ?p ?o }
          | }""".stripMargin

    val results = eval(query, EmptyBindingSet.getInstance)

    // TODO fix RemoteQuery such that ?type will be returned
    expectAllPredicates(results)
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
  //    val query = """
  //        | SELECT DISTINCT * WHERE {
  //        |   BIND (IRI("http://localhost:8001/sparql") AS ?endpoint)
  //        |   SERVICE ?endpoint { [] ?p [] }
  //        | }
  //        |""".stripMargin
  //  }
  //
  //  it should "use an endpoint URI defines via VALUES" in {
  //    val query = """
  //        | SELECT DISTINCT * WHERE {
  //        |   VALUES ?endpoint { <http://localhost:8001/sparql> }
  //        |   SERVICE ?endpoint { [] ?p [] }
  //        | }
  //        |""".stripMargin
  //  }
  //
  //  "A JOIN query" should "return joined results" in {
  //    val query = "SELECT DISTINCT ?p WHERE {[] ?p ?x. ?x a []} LIMIT 3"
  //  }
}
