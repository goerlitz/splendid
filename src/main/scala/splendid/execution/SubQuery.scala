package splendid.execution

import org.openrdf.query.QueryLanguage
import org.openrdf.repository.sparql.SPARQLRepository
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import splendid.Done
import splendid.Query
import splendid.Result
import org.openrdf.query.parser.QueryParserUtil

class SubQuery(parent: ActorRef, endpointUrl: String) extends Actor with ActorLogging {

  val repo = new SPARQLRepository(endpointUrl);
  repo.initialize()

  def receive = {
    case Query(query) => {
      // handle exception
      val tq = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, query, null)

      log.info("QUERY: {}", tq)

      val result = tq.evaluate();
      while (result.hasNext()) {
        parent ! Result(result.next())
      }
      result.close()
      repo.shutDown()
      parent ! Done
    }
  }
}

class Listener extends Actor with ActorLogging {
  def receive = {
    case Result(bindings) => {
      log.info(s"RESULT: $bindings")
    }
    case Done => {
      log.info("Done")
      context.system.shutdown()
    }
  }
}

object QueryApp extends App {

  val endpointUrl = "http://chebi.bio2rdf.org/sparql"
  val sparqlQuery = "SELECT DISTINCT count(?p) as ?count WHERE {[] ?p <http://www.w3.org/2000/01/rdf-schema#Class>}"
  //  val sparqlQuery = "SELECT DISTINCT count(?t) as ?count WHERE {[] a ?t}"
  //  val sparqlQuery = "SELECT DISTINCT ?t WHERE {[] a ?t} LIMIT 20"

  val query = """
    SELECT * WHERE {
      SERVICE <http://depedia.org> {
        [] ?p []
      }
      SERVICE <http://geonames.org> {
        [] ?p []
      }
    }
    """

  val parsed = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, query, null)
  println(s"parsed query: $parsed")

  val system = ActorSystem("myQuery")

  val listener = system.actorOf(Props[Listener], "listener")
  val subQuery = system.actorOf(Props(new SubQuery(listener, endpointUrl)), "subquery")

  subQuery ! Query(sparqlQuery)

  system.awaitTermination()
}