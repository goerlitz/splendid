package splendid.execution

import org.openrdf.query.TupleQueryResult

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Status.Failure
import akka.pattern.pipe
import splendid.Result
import splendid.execution.Execution.Done
import splendid.execution.util.EndpointClient
import splendid.execution.util.SparqlEndpointClient

case class SparqlQuery(endpoint: String, query: String)

case class SparqlTupleResult(client: SparqlEndpointClient, result: TupleQueryResult)

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
    case Failure(err: Throwable) => context.parent ! Execution.Error(err)
    case x                       => println(s"IGNORING: $x") // TODO ignore or send error message
  }

}
