package splendid.execution.util

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import org.openrdf.query.BindingSet
import org.openrdf.query.QueryEvaluationException
import org.openrdf.query.impl.EmptyBindingSet
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpecLike

import akka.actor.ActorSystem
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit

import splendid.common.RDF
import splendid.execution.util.RemoteExecutor.BooleanResult
import splendid.execution.util.RemoteExecutor.EndOfData
import splendid.execution.util.RemoteExecutor.SparqlQuery
import splendid.execution.util.RemoteExecutor.TupleResult

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
    system.awaitTermination(10 seconds)
  }

  "A RemoteExecutor" when {

    val SELECT_PRED = "SELECT DISTINCT * WHERE { [] ?p [] }"

    s"given '$SELECT_PRED'" should {

      "return all predicates as result if no bindings are applied" in {
        evalQuery(EndpointUri, SELECT_PRED, EmptyBindings)

        expectTupleResults(TestData.AllPredicates.map { "p" -> RDF.URI(_) }: _*)
        expectMsg(EndOfData)
      }

      "return one empty result if a predicate binding is applied (variable is replaced)" in {
        val p = SparqlResult.bindings("p" -> RDF.URI(TestData.AllPredicates.head))
        evalQuery(EndpointUri, SELECT_PRED, p)

        expectMsg(TupleResult(EmptyBindings))
        expectMsg(EndOfData)
      }

      "return all predicates as result if no bindings can be applied (wrong variable)" in {
        val s = SparqlResult.bindings("s" -> RDF.URI(TestData.AllSubjects.head))
        evalQuery(EndpointUri, SELECT_PRED, s)

        expectTupleResults(TestData.AllPredicates.map { "p" -> RDF.URI(_) }: _*)
        expectMsg(EndOfData)
      }
    }

    val ASK_PRED = "ASK { [] ?p [] }"
    // TODO: enable ASK tests once LinkedDataServer is fixed

    s"given '$ASK_PRED'" should {

      "return true if no bindings are applied" ignore {
        evalQuery(EndpointUri, ASK_PRED, EmptyBindings)

        expectMsg(BooleanResult(true))
        expectMsg(EndOfData)
      }

      "return true if a predicate binding is applied (variable is replaced)" ignore {
        val p = SparqlResult.bindings("p" -> RDF.URI(TestData.AllPredicates.head))
        evalQuery(EndpointUri, ASK_PRED, p)

        expectMsg(BooleanResult(true))
        expectMsg(EndOfData)
      }

      "return true if no bindings can be applied (wrong variable)" ignore {
        val s = SparqlResult.bindings("s" -> RDF.URI(TestData.AllSubjects.head))
        evalQuery(EndpointUri, ASK_PRED, s)

        expectMsg(BooleanResult(true))
        expectMsg(EndOfData)
      }
    }

    // TODO: what about passing bindings which contain additional variables that are not part of the query - should they be returned?

    val SELECT_SUBJ_PRED = "SELECT DISTINCT * WHERE { ?s ?p [] }"

    s"given '$SELECT_SUBJ_PRED'" should {

      "return all subjects as result if a predicate binding is applied" in {
        val p = SparqlResult.bindings("p" -> RDF.URI(TestData.AllPredicates.head))
        evalQuery(EndpointUri, SELECT_SUBJ_PRED, p)

        expectTupleResults(TestData.AllSubjects.map { "s" -> RDF.URI(_) }: _*)
        expectMsg(EndOfData)
      }

      "return all subjects as result for every applied predicate binding" in {
        TestData.AllPredicates.foreach { p =>
          {
            evalQuery(EndpointUri, SELECT_SUBJ_PRED, SparqlResult.bindings("p" -> RDF.URI(p)))
            expectTupleResults(TestData.AllSubjects.map { s => "s" -> RDF.URI(s) }: _*)
            expectMsg(EndOfData)
          }
        }
      }
    }

    "given an invalid SPARQL endpoint" should {
      "return an exception" in {
        evalQuery("http://loclahost:1", SELECT_PRED, EmptyBindings)
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

  private def evalQuery(endpoint: String, query: String, bindings: BindingSet): Unit =
    TestActorRef(RemoteExecutor.props(endpoint)) ! SparqlQuery(query, bindings)

  private def expectTupleResults(tuples: (String, Any)*): Unit =
    tuples.foreach(x => expectMsg(TupleResult(SparqlResult.bindings(x))))

}
