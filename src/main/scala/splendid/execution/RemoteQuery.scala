package splendid.execution

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success

import org.openrdf.query.QueryLanguage
import org.openrdf.query.TupleQueryResult
import org.openrdf.repository.RepositoryException
import org.openrdf.repository.sparql.SPARQLRepository

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.pattern.pipe
import splendid.Done
import splendid.Result

case class SparqlQuery(endpoint: String, query: String)

case class SparqlTupleResult(client: SparqlEndpointClient, result: TupleQueryResult)

trait SparqlEndpointClient {
  def evalTupleQuery(query: String)(implicit exec: ExecutionContext): Future[SparqlTupleResult]
  def close(): Unit
}

// TODO check if the HTTP client can be reused for consecutive queries to the same SPARQL endpoint
class EndpointClient(uri: String) extends SparqlEndpointClient {

  val repo = new SPARQLRepository(uri);

  override def evalTupleQuery(query: String)(implicit exec: ExecutionContext): Future[SparqlTupleResult] = {

    val promise = Promise[SparqlTupleResult]

    // async tuple query evaluation
    Future {
      try {
        repo.initialize()
        val con = repo.getConnection()

        try {
          // TODO check if use result handler gives better performance
          val result = con.prepareTupleQuery(QueryLanguage.SPARQL, query, null).evaluate()
          promise.success(SparqlTupleResult(this, result))
        } catch {
          case t: Throwable =>
            con.close()
            repo.shutDown()
            promise.failure(t)
        }

      } catch {
        case re: RepositoryException =>
          repo.shutDown()
          promise.failure(re)
      }
    }

    promise.future
  }

  override def close(): Unit = repo.shutDown()
}

object EndpointClient {
  def apply(uri: String): EndpointClient = new EndpointClient(uri)
}

// TODO add SPARQL endpoint client as parameter to allow for better unit testing
class RemoteQuery extends Actor with ActorLogging {

  implicit val exec = context.dispatcher

  def receive = {
    case SparqlQuery(uri, query) => EndpointClient(uri) evalTupleQuery query pipeTo self
    case SparqlTupleResult(client, result) =>
      while (result.hasNext()) {
        context.parent ! Result(result.next())
      }
      result.close()
      client.close()
      context.parent ! Done
    case Failure(err) => println("err: {}", err) // TODO propagate error
    case _ => // TODO ignore or send error message
  }

}
