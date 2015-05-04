package splendid.execution

object Execution {
  
  case object Done
  case class Error(t: Throwable)

}