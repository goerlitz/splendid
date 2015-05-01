package splendid.execution

import org.openrdf.query.QueryLanguage
import org.openrdf.repository.sparql.SPARQLRepository

import akka.actor.Actor
import akka.actor.ActorLogging
import splendid.Done
import splendid.Result

case class SparqlQuery(endpoint: String, query: String)

class RemoteQuery extends Actor with ActorLogging {

  def receive = {
    case SparqlQuery(endpoint, query) => {

      // TODO: make asynchronous
//      log.info(s"executing query '$query' at $endpoint")

      // prepare connection to SPARQL endpoint
      val repo = new SPARQLRepository(endpoint);

      // TODO: handle exception
      try {
        repo.initialize()
        val con = repo.getConnection()

        try {
          val tq = con.prepareTupleQuery(QueryLanguage.SPARQL, query, null)

          // TODO: handle exceptions
          val result = tq.evaluate();
          while (result.hasNext()) {
            context.parent ! Result(result.next())
          }
          result.close()
        } catch {
          case e: Throwable => // TODO: handle exceptions
        } finally {
          con.close()
        }
      } catch {
        case e: Throwable => // TODO: handle exceptions
      } finally {
        repo.shutDown()
      }
      context.parent ! Done // always send DONE?
    }
    case _ => // TODO ignore or send error message
  }
}