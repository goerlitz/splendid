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

object RemoteExecutor {
  final case class SparqlQuery(query: String, bindings: BindingSet)
  final case class TupleResult(bindings : BindingSet)
  final case class BooleanResult(exist: Boolean)
  final case object EndOfData // TODO: include count of sent results?

  def props(endpoint: String): Props = Props(new RemoteExecutor(endpoint))
}

/**
 * An actor which controls the asynchronous execution of SPARQL queries on a remote SPARQL endpoint.
 *
 * @author Olaf Goerlitz
 */
class RemoteExecutor private (endpoint: String) extends Actor with ActorLogging {

  import RemoteExecutor.{ SparqlQuery, TupleResult, BooleanResult, EndOfData }

  implicit val exec = context.dispatcher

  val repo = new SPARQLRepository(endpoint)

  override def preStart: Unit = repo.initialize
  override def postStop: Unit = Try(repo.shutDown) recover { case t => log.warning(s"repo shutdown failed: $t") }

  override def receive: Actor.Receive = {
    case SparqlQuery(query, bindings) => eval(query, bindings)
    case x                            => ??? // TODO: fail?
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
      context.parent ! TupleResult(result.next()) // TODO can throw exception
    }
    result.close() // TODO can throw exception
    context.parent ! EndOfData
  }

  private def evalBooleanQuery(query: BooleanQuery, bindings: BindingSet): Try[Unit] = Try {
    bindings.map { bs => query.setBinding(bs.getName, bs.getValue) }
    context.parent ! BooleanResult(query.evaluate())
    context.parent ! EndOfData
  }

}
