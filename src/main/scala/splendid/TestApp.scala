package splendid

//import scalaj.http.Http
//import scalaj.http.HttpOptions
import org.openrdf.repository.sparql.SPARQLRepository
import org.openrdf.query.QueryLanguage
import org.openrdf.query.TupleQueryResult
import org.openrdf.query.BindingSet
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ActorLogging
import collection.JavaConversions._

case class Query(query: String)
case class Result(bs: BindingSet)
case object Init
case object Done

class HashJoinActor extends Actor with ActorLogging {
  def receive = {
    case Init => log.info("initializing")
    case Result(bs) => log.info("got values" + (bs.getBindingNames map { b => b + "-> " + bs.getValue(b).stringValue()}))
  }
}

object TestApp extends App {

  val endpointUrl = "http://chebi.bio2rdf.org/sparql"
  val sparqlQuery = "SELECT DISTINCT ?p WHERE {[] ?p []} LIMIT 3"

  val system = ActorSystem("mySparqlActors")
  val join = system.actorOf(Props[HashJoinActor], "join")
  
  join ! Init

  //  val request = Http("http://dbpedia.org/sparql").param("query", "SELECT DISTINCT ?p WHERE {[] ?p []} LIMIT 3")
  //  val request = Http("http://dbpedia.org/sparql")
  //  val request = Http("http://chebi.bio2rdf.org/sparql")
  //    .param("query", "SELECT DISTINCT ?p WHERE {[] ?p []} LIMIT 3")
  //    .options(HttpOptions.connTimeout(5000), HttpOptions.readTimeout(60000)).param("format", "XML")

  //  println(s"query=$request")
  //  val result = request.asString
  //  println(s"result=$result")

  val result = query(endpointUrl, sparqlQuery)

  while (result.hasNext()) {
    val bindingSet = result.next()
    val value = bindingSet.getValue("p")
    println(value.stringValue())

    join ! Result(bindingSet)

  }

  /**
   * @param endpoint
   * @param query
   * @return
   */
  def query(endpoint: String, query: String): TupleQueryResult = {
    val repo = new SPARQLRepository(endpointUrl);
    repo.initialize()
    val tupleQuery = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery, null);
    tupleQuery.evaluate();
  }

}