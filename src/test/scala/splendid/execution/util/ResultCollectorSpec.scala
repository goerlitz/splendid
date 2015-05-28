package splendid.execution.util

import java.util.concurrent.TimeoutException

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationDouble
import scala.language.postfixOps

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import akka.util.Timeout

/**
 * Testing various conditions of the ResultCollector, including empty result set, postponed messages, and handling of tell and ask messages.
 *
 * @author Olaf Goerlitz
 */
class ResultCollectorSpec extends TestKit(ActorSystem("IterationTest"))
  with FlatSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {

  implicit val timeout = Timeout(5 seconds) // required for '?'
  implicit val context = scala.concurrent.ExecutionContext.global

  override def afterAll: Unit = system.shutdown()

  private def expectTimeout(f: Future[Any]): Future[Any] = {
    intercept[TimeoutException] { Await.result(f, 0.3 seconds) }
    f
  }

  import ResultCollector.{ HasNext, GetNext, Result, Done }

  private def expectResult(f: Future[Any], value: Any): Unit = Await.result(f, 1 second) match {
    case b: Boolean   => b should be(value)
    case r: Result    => r should be(value)
    // FIXME use function parameter for comparison
    case e: Throwable => e shouldBe a[NoSuchElementException]
    case x            => throw new UnsupportedOperationException(s"$x")
  }

  /**
   * Using a synchronous test actor for each test
   */
  private trait TestActor {
    val actorRef = TestActorRef(ResultCollector.props)
  }

  "A ResultCollector" should "return false for !HasNext if the result set is empty" in new TestActor {
    actorRef ! HasNext // before Done
    actorRef ! Done
    expectMsg(false)
    actorRef ! HasNext // after Done
    expectMsg(false)
  }

  it should "return false for ?HasNext if the result set is empty" in new TestActor {
    val future = expectTimeout(actorRef ? HasNext)
    actorRef ! Done
    expectResult(future, false)             // before Done
    expectResult(actorRef ? HasNext, false) // after Done
  }

  it should "return an exception for !GetNext if the result set is empty" in new TestActor {
    actorRef ! GetNext // before Done
    actorRef ! Done
    expectMsgPF() { case e: NoSuchElementException => () }
    actorRef ! GetNext // after Done
    expectMsgPF() { case e: NoSuchElementException => () }
  }

  it should "return an exception for ?GetNext if the result set is empty" in new TestActor {
    val future = expectTimeout(actorRef ? GetNext)
    actorRef ! Done
    expectResult(future, None)             // before Done
    expectResult(actorRef ? GetNext, None) // after Done
  }

  it should "receive postponed responses for !HasNext and !GetNext" in new TestActor {
    // no result available and not done yet -> messages are postponed
    actorRef ! HasNext; intercept[AssertionError] { expectMsg(0.3 seconds, true) }
    actorRef ! GetNext; intercept[AssertionError] { expectMsg(0.3 seconds, Result("Hello")) }

    // next result -> receive postponed messages
    actorRef ! Result("Hello");
    expectMsg(true)
    expectMsg(Result("Hello"))
  }

  it should "receive postponed responses for ?HasNext and ?GetNext" in new TestActor {
    // no result available and not done yet -> messages are postponed
    val futureHasNext = expectTimeout(actorRef ? HasNext)
    val futureGetNext = expectTimeout(actorRef ? GetNext)

    // next result -> receive postponed messages
    actorRef ! Result("Hello");
    expectResult(futureHasNext, true)
    expectResult(futureGetNext, Result("Hello"))
  }

  it should "immediately receive responses for !HasNext and !GetNext if a result is available" in new TestActor {
    // first result
    actorRef ! Result("Hello")
    actorRef ! HasNext; expectMsg(true)
    actorRef ! GetNext; expectMsg(Result("Hello"))

    // last result
    actorRef ! Result("Goodbye")
    actorRef ! Done
    actorRef ! HasNext; expectMsg(true)
    actorRef ! GetNext; expectMsg(Result("Goodbye"))
    actorRef ! HasNext; expectMsg(false)
    actorRef ! GetNext; expectMsgPF() { case e: NoSuchElementException => () }
  }

  it should "immediately receive responses for ?HasNext and ?GetNext if a result is available" in new TestActor {
    // first result
    actorRef ! Result("Hello")
    expectResult(actorRef ? HasNext, true)
    expectResult(actorRef ? GetNext, Result("Hello"))

    // last result
    actorRef ! Result("Goodbye")
    actorRef ! Done
    expectResult(actorRef ? HasNext, true)
    expectResult(actorRef ? GetNext, Result("Goodbye"))
    expectResult(actorRef ? HasNext, false)
    expectResult(actorRef ? GetNext, None)
  }

  it should "receive results from a child actor" in {

    // must use system actor - TestActorRef does not work well with Stash :-(
    // it fails if HasNext is received before the first Result from TestResultSender
    val actorRef = system.actorOf(ResultCollector.props(Props(new TestResultSender("Hello", 1))))

    actorRef ! HasNext; expectMsg(true)
    actorRef ! GetNext; expectMsg(Result("Hello"))
    actorRef ! GetNext; expectMsg(Result(1))
    actorRef ! HasNext; expectMsg(false)
  }

  protected class TestResultSender(results: Any*) extends Actor {
    def receive: Actor.Receive = PartialFunction.empty

    results.foreach { context.parent ! Result(_) }
    context.parent ! Done
  }
}
