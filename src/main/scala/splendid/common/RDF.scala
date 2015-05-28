package splendid.common

object RDF {
  case class URI(uri: String)
  case class BNODE(nodeID: String)
  case class LITERAL(value: String)
}
