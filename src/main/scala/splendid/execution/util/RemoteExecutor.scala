package splendid.execution.util

import org.openrdf.query.BindingSet

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Status.Failure
import akka.pattern.pipe

import splendid.execution.RemoteQuery.SparqlTupleResult
import splendid.execution.util.ResultCollector.Done
import splendid.execution.util.ResultCollector.Result

object RemoteExecutor {
  final case class SparqlQuery(endpoint: String, query: String, bindings: BindingSet)

  def props(): Props = Props[RemoteExecutor]
}

/**
 * An actor which controls the execution of remote SPARQL queries.
 *
 * @author Olaf Goerlitz
 */
class RemoteExecutor extends Actor {

  private val endpoints = scala.collection.mutable.Map.empty[String, SparqlEndpointClient]

  implicit val exec = context.dispatcher

  import RemoteExecutor.SparqlQuery

  def receive: Actor.Receive = {
    case SparqlQuery(endpoint, query, bindings) =>
      client(endpoint).evalTupleQuery(query, bindings) pipeTo self
    case SparqlTupleResult(client, result) =>
      // TODO make this asynchronous - actor blocks while processing large result set
      while (result.hasNext()) {
        context.parent ! Result(result.next())
      }
      result.close()
      context.parent ! Done
    case Failure(err: Throwable) => context.parent ! err
    case x                       => ??? // TODO failure?
  }

  private def client(url: String) = endpoints.getOrElseUpdate(url, EndpointClient(url))
}
