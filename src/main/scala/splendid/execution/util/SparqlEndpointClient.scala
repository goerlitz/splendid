package splendid.execution.util

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise

import org.openrdf.query.BindingSet
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryException
import org.openrdf.repository.sparql.SPARQLRepository

import splendid.execution.RemoteQuery.SparqlTupleResult

trait SparqlEndpointClient {
  def evalTupleQuery(query: String, bindings: BindingSet)(implicit exec: ExecutionContext): Future[SparqlTupleResult]
  def close(): Unit
}

// TODO check if the HTTP client can be reused for consecutive queries to the same SPARQL endpoint
class EndpointClient(uri: String) extends SparqlEndpointClient {

  val repo = new SPARQLRepository(uri);

  override def evalTupleQuery(query: String, bindings: BindingSet)(implicit exec: ExecutionContext): Future[SparqlTupleResult] = {

    val promise = Promise[SparqlTupleResult]

    // async tuple query evaluation
    Future {
      try {
        repo.initialize()
        val con = repo.getConnection()

        try {
          // TODO check if using a result handler gives better performance
          val tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, query, null)
          for (bs <- bindings) tupleQuery.setBinding(bs.getName, bs.getValue)
          val result = tupleQuery.evaluate()
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