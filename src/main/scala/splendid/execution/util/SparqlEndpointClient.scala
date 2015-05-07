package splendid.execution.util

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise

import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryException
import org.openrdf.repository.sparql.SPARQLRepository

import splendid.execution.SparqlTupleResult

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