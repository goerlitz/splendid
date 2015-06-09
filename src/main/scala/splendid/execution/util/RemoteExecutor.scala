package splendid.execution.util

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.openrdf.query.BindingSet
import org.openrdf.query.QueryLanguage
import org.openrdf.query.TupleQueryResult
import org.openrdf.repository.RepositoryConnection
import org.openrdf.repository.sparql.SPARQLRepository

import akka.actor.Actor
import akka.actor.Props
import splendid.execution.util.ResultCollector.Done
import splendid.execution.util.ResultCollector.Result

object RemoteExecutor {
  final case class SparqlQuery(endpoint: String, query: String, bindings: BindingSet)

  def props(): Props = Props[RemoteExecutor]
}

/**
 * An actor which controls the asynchronous execution of remote SPARQL queries.
 *
 * @author Olaf Goerlitz
 */
class RemoteExecutor extends Actor {

  implicit val exec = context.dispatcher

  import RemoteExecutor.SparqlQuery

  def receive: Actor.Receive = {
    case SparqlQuery(endpoint, query, bindings) => eval(endpoint, query, bindings)
    case x                                      => ??? // TODO failure?
  }

  private def eval(endpoint: String, query: String, bindings: BindingSet): Unit = {

    // async tuple query evaluation
    Future {

      // TODO reuse endpoint/connection
      val repo = new SPARQLRepository(endpoint);

      Try(repo.initialize()).map(_ => repo.getConnection()) match {
        case Failure(t) =>
          repo.shutDown() // TODO can throw exception
          context.parent ! t // TODO implement proper error handling
        case Success(con) =>
          evalQuery(con, query, bindings) match {
            case Failure(t) => context.parent ! t // TODO implement proper error handling
            case Success(result) =>
              while (result.hasNext()) { // TODO can throw exception
                context.parent ! Result(result.next()) // TODO can throw exception
              }
              result.close() // TODO can throw exception
              context.parent ! Done
          }
          con.close() // TODO can throw exception
          repo.shutDown() // TODO can throw exception
      }
    }
  }

  private def evalQuery(con: RepositoryConnection, query: String, bindings: BindingSet): Try[TupleQueryResult] = for (
    tupleQuery <- Try(con.prepareTupleQuery(QueryLanguage.SPARQL, query, null));
    boundQuery <- Try({ bindings.map { bs => tupleQuery.setBinding(bs.getName, bs.getValue) }; tupleQuery });
    result <- Try(boundQuery.evaluate())
  ) yield result

}
