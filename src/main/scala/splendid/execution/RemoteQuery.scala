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

case class SparqlTupleResult(client: SparqlEndpointClient, result: TupleQueryResult)

/**
 * Actor for executing a SPARQL query on a SPARQL endpoint.
 *
 * Each [[BindingSet]] of the result is forwarded to the parent actor.
 *
 * @author Olaf Goerlitz
 */
class RemoteQuery(client: SparqlEndpointClient, query: String) extends Actor with ActorLogging {

  implicit val exec = context.dispatcher

  client evalTupleQuery query pipeTo self

  def receive = {
    case SparqlTupleResult(client, result) =>
      while (result.hasNext()) {
        context.parent ! Result(result.next())
      }
      result.close()
      context.parent ! Done
    case Failure(err: Throwable) => context.parent ! Execution.Error(err)
    case x                       => println(s"IGNORING: $x") // TODO ignore or send error message
  }

}
