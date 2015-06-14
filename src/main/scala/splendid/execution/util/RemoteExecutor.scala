package splendid.execution.util

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.concurrent.Future
import scala.util.Try

import org.openrdf.query.BindingSet
import org.openrdf.query.BooleanQuery
import org.openrdf.query.GraphQuery
import org.openrdf.query.Query
import org.openrdf.query.QueryLanguage
import org.openrdf.query.TupleQuery
import org.openrdf.repository.sparql.SPARQLRepository

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import splendid.execution.util.ResultCollector.Done
import splendid.execution.util.ResultCollector.Result

object RemoteExecutor {
  final case class SparqlQuery(query: String, bindings: BindingSet)

  def props(endpoint: String): Props = Props(new RemoteExecutor(endpoint))
}

/**
 * An actor which controls the asynchronous execution of SPARQL queries on a remote SPARQL endpoint.
 *
 * @author Olaf Goerlitz
 */
class RemoteExecutor private (endpoint: String) extends Actor with ActorLogging {

  import RemoteExecutor.SparqlQuery

  implicit val exec = context.dispatcher

  val repo = new SPARQLRepository(endpoint)

  override def preStart: Unit = repo.initialize
  override def postStop: Unit = Try(repo.shutDown) recover { case t => log.warning(s"repo shutdown failed: $t") }

  override def receive: Actor.Receive = {
    case SparqlQuery(query, bindings) => eval(query, bindings)
    case x                            => ??? // TODO failure?
  }

  private def eval(query: String, bindings: BindingSet) = Future {

    try {
      val con = repo.getConnection()

      Try(con.prepareQuery(QueryLanguage.SPARQL, query)) flatMap evalQuery(bindings) recover {
        case t: Throwable => context.parent ! t
      }
      con.close()

    } catch {
      case t: Throwable => context.parent ! t
    }
  }

  private def evalQuery(bindings: BindingSet): PartialFunction[Query, Try[Unit]] = {
    case q: TupleQuery   => evalTupleQuery(q, bindings)
    case q: GraphQuery   => ???
    case q: BooleanQuery => evalBooleanQuery(q, bindings)
  }

  private def evalTupleQuery(query: TupleQuery, bindings: BindingSet): Try[Unit] = Try {
    bindings.map { bs => query.setBinding(bs.getName, bs.getValue) }
    query.evaluate()
  } map { result =>
    while (result.hasNext()) { // TODO can throw exception
      context.parent ! Result(result.next()) // TODO can throw exception
    }
    result.close() // TODO can throw exception
    context.parent ! Done
  }

  private def evalBooleanQuery(query: BooleanQuery, bindings: BindingSet): Try[Unit] = Try {
    bindings.map { bs => query.setBinding(bs.getName, bs.getValue) }
    query.evaluate()
    ??? // TODO: send ASK response
  }

}
