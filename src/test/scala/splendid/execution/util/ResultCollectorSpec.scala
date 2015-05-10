package splendid.execution.util

import java.util.concurrent.TimeoutException
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.util.Timeout
import scala.concurrent.Future
import akka.testkit.TestActorRef
import scala.util.Success
import scala.util.Failure

/**
 * Testing various conditions of the ResultCollector, including empty result set, postponed messages, and handling of tell and ask messages.
 * 
 * @author Olaf Goerlitz
 */
class ResultCollectorSpec extends TestKit(ActorSystem("IterationTest")) with FlatSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {

  implicit val timeout = Timeout(5 seconds) // required for '?'
  implicit val context = scala.concurrent.ExecutionContext.global

  override def afterAll = system.shutdown()

  private def expectTimeout(f: Future[Any]): Future[Any] = {
    intercept[TimeoutException] { Await.result(f, 1 second) }
    f
  }

  private def expectResult(f: Future[Any], value: Any): Unit = Await.result(f, 1 second) match {
    case b: Boolean   => b should be(value)
    case r: Result    => r should be(value)
    // FIXME use function parameter for comparison
    case e: Throwable => e shouldBe a[NoSuchElementException]
    case x            => throw new UnsupportedOperationException(s"$x")
  }

  "An empty result" should "return false for !HasNext" in {
    val actorRef = TestActorRef(new ResultCollector())
    actorRef ! HasNext // before Done
    actorRef ! Done
    expectMsg(false)
    actorRef ! HasNext // after Done
    expectMsg(false)
  }

  it should "return false for ?HasNext" in {
    val actorRef = TestActorRef(new ResultCollector())
    val future = expectTimeout(actorRef ? HasNext)
    actorRef ! Done
    expectResult(future, false)             // before Done
    expectResult(actorRef ? HasNext, false) // after Done
  }

  it should "return an exception for !GetNext" in {
    val actorRef = TestActorRef(new ResultCollector())
    actorRef ! GetNext // before Done
    actorRef ! Done
    expectMsgPF() { case e: NoSuchElementException => () }
    actorRef ! GetNext // after Done
    expectMsgPF() { case e: NoSuchElementException => () }
  }

  it should "return an exception for ?GetNext" in {
    val actorRef = TestActorRef(new ResultCollector())
    val future = expectTimeout(actorRef ? GetNext)
    actorRef ! Done
    expectResult(future, None)             // before Done
    expectResult(actorRef ? GetNext, None) // after Done
  }

  "A ResultCollector" should "handle !HasNext and !GetNext correctly" in {

    val actorRef = TestActorRef(new ResultCollector())

    actorRef ! Result("Hello")
    actorRef ! HasNext; expectMsg(true)
    actorRef ! GetNext; expectMsg(Result("Hello"))

    // no result available and not done yet -> messages are postponed
    actorRef ! HasNext; intercept[AssertionError] { expectMsg(1 second, true) }
    actorRef ! GetNext; intercept[AssertionError] { expectMsg(1 second, Result("Hello")) }

    // next result -> expect postponed messages
    actorRef ! Result("Hello 2");
    expectMsg(true)
    expectMsg(Result("Hello 2"))

    // last result
    actorRef ! Result("Hello 3")
    actorRef ! Done
    actorRef ! HasNext; expectMsg(true)
    actorRef ! GetNext; expectMsg(Result("Hello 3"))
    actorRef ! HasNext; expectMsg(false)
    actorRef ! GetNext; expectMsgPF() { case e: NoSuchElementException => () }
  }
  
  it should "handle ?HasNext and ?GetNext correctly" in {

    val actorRef = TestActorRef(new ResultCollector())
    
    actorRef ! Result("Hello")
    expectResult(actorRef ? HasNext, true)
    expectResult(actorRef ? GetNext, Result("Hello"))
    
    // no result available and not done yet -> messages are postponed
    val futureHasNext = expectTimeout(actorRef ? HasNext)
    val futureGetNext = expectTimeout(actorRef ? GetNext)
    
    // next result -> expect postponed messages
    actorRef ! Result("Hello 2");
    expectResult(futureHasNext, true)
    expectResult(futureGetNext, Result("Hello 2"))

    // last result
    actorRef ! Result("Hello 3")
    actorRef ! Done
    expectResult(actorRef ? HasNext, true)
    expectResult(actorRef ? GetNext, Result("Hello 3"))
    expectResult(actorRef ? HasNext, false)
    expectResult(actorRef ? GetNext, None)
  }

}