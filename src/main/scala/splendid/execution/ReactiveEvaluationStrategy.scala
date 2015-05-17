package splendid.execution

import org.openrdf.model.Resource
import org.openrdf.model.Statement
import org.openrdf.model.URI
import org.openrdf.model.Value
import org.openrdf.model.ValueFactory
import org.openrdf.model.impl.ValueFactoryImpl
import org.openrdf.query.BindingSet
import org.openrdf.query.QueryEvaluationException
import org.openrdf.query.QueryLanguage
import org.openrdf.query.algebra.Join
import org.openrdf.query.algebra.Service
import org.openrdf.query.algebra.TupleExpr
import org.openrdf.query.algebra.evaluation.TripleSource
import org.openrdf.query.algebra.evaluation.federation.FederatedServiceResolverImpl
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl
import org.openrdf.query.impl.EmptyBindingSet
import org.openrdf.query.parser.QueryParserUtil

import akka.actor.ActorSystem
import akka.actor.Props
import info.aduna.iteration.CloseableIteration
import info.aduna.iteration.EmptyIteration
import splendid.execution.util.ResultCollector
import splendid.execution.util.ResultStreamIteration

object ReactiveEvaluationStrategy {

  type BindingSetIteration = CloseableIteration[BindingSet, QueryEvaluationException]
  type StatementIteration = CloseableIteration[Statement, QueryEvaluationException]

  def apply() = new ReactiveEvaluationStrategy(EmptyTripleSource)

  /**
   * A TripleSource which contains nothing.
   */
  private object EmptyTripleSource extends TripleSource {
    @throws(classOf[QueryEvaluationException])
    override def getStatements(subj: Resource, pred: URI, obj: Value, contexts: Resource*): StatementIteration = new EmptyIteration()
    override def getValueFactory(): ValueFactory = ValueFactoryImpl.getInstance
  }
}

class ReactiveEvaluationStrategy private (tripleSource: TripleSource) extends EvaluationStrategyImpl(tripleSource, new FederatedServiceResolverImpl()) {

  import ReactiveEvaluationStrategy._

  // TODO shutdown actor system
  val system = ActorSystem.create("EvaluationStrategy")

  @throws(classOf[QueryEvaluationException])
  override def evaluate(service: Service, bindings: BindingSet): BindingSetIteration = {

    // ensure that a service URI is present
    val serviceRef = service.getServiceRef
    val endpointUrl = serviceRef.hasValue match {
      case true => serviceRef.getValue.stringValue
      case false =>
        val varName = serviceRef.getName
        if (!bindings.hasBinding(varName))
          throw new QueryEvaluationException
        else
          bindings.getValue(varName).stringValue
    }
    val queryString = service.getSelectQueryString(service.getServiceVars)

    val collectorProps = ResultCollector.props(RemoteQuery.props(endpointUrl, queryString, bindings))
    new ResultStreamIteration(system.actorOf(collectorProps));
  }

  @throws(classOf[QueryEvaluationException])
  override def evaluate(join: Join, bindings: BindingSet): BindingSetIteration = ???
}
