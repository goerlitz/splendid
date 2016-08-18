package splendid.execution.util

import scala.collection.JavaConversions.seqAsJavaList

import org.openrdf.model.impl.SimpleValueFactory
import org.openrdf.query.BindingSet
import org.openrdf.query.impl.ListBindingSet

import splendid.common.RDF

object SparqlResult {

  val fac = SimpleValueFactory.getInstance

  def bindings(tuples: (String, Any)*): BindingSet = {
    val (names, values) = tuples.unzip
    new ListBindingSet(names.toList, values.map {
      case RDF.URI(uri)       => fac.createIRI(uri)
      case RDF.BNODE(nodeID)  => fac.createBNode(nodeID)
      case RDF.LITERAL(value) => fac.createLiteral(value)
      case anyObject          => fac.createLiteral(anyObject.toString())
    })
  }
}
