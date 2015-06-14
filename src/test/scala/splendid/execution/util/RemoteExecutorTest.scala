package splendid.execution.util

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import org.openrdf.query.BindingSet
import org.openrdf.query.QueryEvaluationException
import org.openrdf.query.impl.EmptyBindingSet
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpecLike

import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.ImplicitSender
import akka.testkit.TestKit

import splendid.common.RDF
import splendid.execution.testutil.FosterParent
import splendid.execution.util.RemoteExecutor.SparqlQuery
import splendid.execution.util.ResultCollector.Done
import splendid.execution.util.ResultCollector.Result

/**
 * Behavior-driven tests for RemoteExecutor.
 *
 * @author Olaf Goerlitz
 */
class RemoteExecutorTest extends TestKit(ActorSystem("RemoteExecutor"))
    with WordSpecLike with BeforeAndAfterAll with ImplicitSender {

  val EndpointUri = TestData.startSparqlEndpoint()
  val EmptyBindings = EmptyBindingSet.getInstance

  override def beforeAll(): Unit = {
    //    testEndpoint.start()
  }

  override def afterAll(): Unit = {
    //    testEndpoint.stop()
    system.shutdown()
    system.awaitTermination(100 seconds)
  }

  "A RemoteExecutor" when {

    "given triple pattern '[] ?p []'" should {

      "return all predicates as result if no bindings are applied" in {
        evalQuery(EndpointUri, "SELECT DISTINCT * WHERE { [] ?p [] }", EmptyBindings)

        expectResultBindings(TestData.AllPredicates.map { "p" -> RDF.URI(_) }: _*)
        expectMsg(Done)
      }

      "return one empty result if a predicate binding is applied (variable get replaced)" in {
        val p = SparqlResult.bindings("p" -> RDF.URI(TestData.AllPredicates.head))
        evalQuery(EndpointUri, "SELECT DISTINCT * WHERE { [] ?p [] }", p)

        expectMsg(Result(EmptyBindings))
        expectMsg(Done)
      }

      "return all predicates as result if no bindings can be applied (wrong variable)" in {
        val s = SparqlResult.bindings("s" -> RDF.URI(TestData.AllSubjects.head))
        evalQuery(EndpointUri, "SELECT DISTINCT * WHERE { [] ?p [] }", s)

        expectResultBindings(TestData.AllPredicates.map { "p" -> RDF.URI(_) }: _*)
        expectMsg(Done)
      }
    }

    // TODO: what about passing bindings which contain additional variables that are not part of the query?

    "given triple pattern '?s ?p []'" should {

      "return all subjects as result if a predicate binding is applied" in {
        val p = SparqlResult.bindings("p" -> RDF.URI(TestData.AllPredicates.head))
        evalQuery(EndpointUri, "SELECT DISTINCT * WHERE { ?s ?p [] }", p)

        expectResultBindings(TestData.AllSubjects.map { "s" -> RDF.URI(_) }: _*)
        expectMsg(Done)
      }

      "return all subjects as result for every applied predicate binding" in {
        TestData.AllPredicates.foreach { p =>
          {
            evalQuery(EndpointUri, "SELECT DISTINCT * WHERE { ?s ?p [] }", SparqlResult.bindings("p" -> RDF.URI(p)))
            expectResultBindings(TestData.AllSubjects.map { s => "s" -> RDF.URI(s) }: _*)
            expectMsg(Done)
          }
        }
      }
    }

    "given an invalid SPARQL endpoint" should {
      "return an exception" in {
        evalQuery("http://loclahost:1", "SELECT * WHERE { [] ?p [] }", EmptyBindings)
        expectMsgPF() { case e: QueryEvaluationException => () }
      }
    }

    "given an invalid triple pattern" should {
      "return an exception" in {
        evalQuery(EndpointUri, "SELECT DISTINCT * WHERE { [] ?p SomethingWrong }", EmptyBindings)
        expectMsgPF() { case e: QueryEvaluationException => () }
      }
    }
  }

  private def evalQuery(endpoint: String, query: String, bindings: BindingSet): Unit = {
    val executor = system.actorOf(FosterParent.props(RemoteExecutor.props(endpoint), testActor))
    executor ! SparqlQuery(query, bindings)
  }

  private def expectResultBindings(tuples: (String, Any)*): Unit = tuples.foreach(x => expectMsg(Result(SparqlResult.bindings(x))))

}
