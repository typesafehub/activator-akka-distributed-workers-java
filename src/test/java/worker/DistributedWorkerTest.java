package worker;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.contrib.pattern.*;
import akka.testkit.JavaTestKit;
import java.util.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.assertEquals;

public class DistributedWorkerTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("DistributedWorkerTest");
  }

  @AfterClass
  public static void teardown() {
    JavaTestKit.shutdownActorSystem(system);
    system = null;
  }

  static FiniteDuration workTimeout = Duration.create(3, "seconds");
  static FiniteDuration registerInterval = Duration.create(1, "second");

  @Test
  public void testWorkers() throws Exception {
    new JavaTestKit(system) {{
      Address clusterAddress = Cluster.get(system).selfAddress();
      Cluster.get(system).join(clusterAddress);
      system.actorOf(ClusterSingletonManager.defaultProps(Master.props(workTimeout), "active",
        PoisonPill.getInstance(), ""), "master");

      Set<ActorSelection> initialContacts = new HashSet<ActorSelection>();
      initialContacts.add(system.actorSelection(clusterAddress + "/user/receptionist"));

      ActorRef clusterClient = system.actorOf(ClusterClient.defaultProps(initialContacts),
        "clusterClient");

      for (int n = 1; n <= 3; n += 1) {
        system.actorOf(Worker.props(clusterClient,
          Props.create(WorkExecutor.class), registerInterval), "worker-" + n);
      }

      ActorRef flakyWorker = system.actorOf(Worker.props(clusterClient,
        Props.create(FlakyWorkExecutor.class), registerInterval), "flaky-worker");

      final ActorRef frontend = system.actorOf(Props.create(Frontend.class), "frontend");

      final JavaTestKit results = new JavaTestKit(system);

      DistributedPubSubExtension.get(system).mediator().tell(
        new DistributedPubSubMediator.Subscribe(Master.ResultsTopic, results.getRef()),
        getRef());
      expectMsgClass(DistributedPubSubMediator.SubscribeAck.class);

      // might take a while for things to get connected
      new AwaitAssert(duration("10 seconds")) {
        protected void check() {
          frontend.tell(new Master.Work("1", 1), getRef());
          expectMsgEquals(Frontend.Ok.getInstance());
        }
      };

      assertEquals(results.expectMsgClass(Master.WorkResult.class).workId, "1");

      for (int n = 2; n <= 100; n += 1) {
        frontend.tell(new Master.Work(Integer.toString(n), n), getRef());
        expectMsgEquals(Frontend.Ok.getInstance());
      }

      results.new Within(duration("10 seconds")) {
        public void run() {
          Object[] messages = results.receiveN(99);
          SortedSet<Integer> set = new TreeSet<Integer>();
          for (Object m: messages) {
            set.add(Integer.parseInt(((Master.WorkResult) m).workId));
          }
          // nothing lost, and no duplicates
          Iterator<Integer> iterator = set.iterator();
          for (int n = 2; n <= 100; n += 1) {
            assertEquals(n, iterator.next().intValue());
          }
        }
      };
    }};
  }

  static class FlakyWorkExecutor extends WorkExecutor {
    int i = 0;

    @Override
    public void postRestart(Throwable reason) throws Exception {
      i = 3;
      super.postRestart(reason);
    }

    @Override
    public void onReceive(Object message) {
      if (message instanceof Integer) {
        Integer n = (Integer) message;
        i += 1;
        if (i == 3) throw new RuntimeException("Flaky worker");
        if (i == 5) getContext().stop(getSelf());
      }
      super.onReceive(message);
    }
  }
}
