package splendid.execution

import org.openrdf.query.BindingSet
import org.openrdf.query.TupleQueryResult

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import akka.actor.Status.Failure
import akka.pattern.pipe
import splendid.execution.util.EndpointClient
import splendid.execution.util.ResultCollector.Done
import splendid.execution.util.ResultCollector.Result
import splendid.execution.util.SparqlEndpointClient

object RemoteQuery {

  case class SparqlTupleResult(client: SparqlEndpointClient, result: TupleQueryResult)

  /**
   * Create Props for an actor of this type.
   *
   * @param endpointURI URI of the SPARQL Endpoint.
   * @param query The query to be evaluated at the SPARQL Endpoint.
   * @return a [[Props]] which can be further configured.
   */
  def props(endpointURI: String, query: String, bindings: BindingSet): Props = {
    // TODO reuse client if multiple requests are sent to the same SPARQL endpoint
    val client = EndpointClient(endpointURI)
    Props(classOf[RemoteQuery], client, query, bindings)
  }
}

/**
 * Actor for executing a SPARQL query on a SPARQL Endpoint.
 *
 * Each query result entry is forwarded to the parent actor.
 *
 * @author Olaf Goerlitz
 */
class RemoteQuery private (client: SparqlEndpointClient, query: String, bindings: BindingSet) extends Actor with ActorLogging {

  implicit val exec = context.dispatcher

  // use pipe pattern to forward result and failure messages
  client evalTupleQuery (query, bindings) pipeTo self

  import RemoteQuery.SparqlTupleResult

  def receive = {
    case SparqlTupleResult(client, result) =>
      while (result.hasNext()) {
        context.parent ! Result(result.next())
      }
      result.close()
      context.parent ! Done
    case Failure(err: Throwable) => context.parent ! err
    case x                       => println(s"IGNORING: $x") // TODO ignore or send error message
  }
}
