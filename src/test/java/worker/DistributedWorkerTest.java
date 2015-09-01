package worker;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.client.ClusterClient;
import akka.cluster.client.ClusterClientSettings;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.testkit.JavaTestKit;
import akka.testkit.TestProbe;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DistributedWorkerTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    Config config = ConfigFactory.load("test.conf");
    try {
      File journalDir = new File(config.getString("akka.persistence.journal.leveldb.dir"));
      FileUtils.deleteDirectory(journalDir);
      File snapshotDir = new File(config.getString("akka.persistence.snapshot-store.local.dir"));
      FileUtils.deleteDirectory(snapshotDir);
    } catch (IOException ex) {
      throw new RuntimeException("Failed to clean up old test database", ex);
    }

    system = ActorSystem.create("DistributedWorkerTest", config);
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
      TestProbe clusterProbe = new TestProbe(system);
      Cluster.get(system).subscribe(clusterProbe.ref(), ClusterEvent.MemberUp.class);
      clusterProbe.expectMsgClass(ClusterEvent.CurrentClusterState.class);

      Address clusterAddress = Cluster.get(system).selfAddress();
      Cluster.get(system).join(clusterAddress);
      clusterProbe.expectMsgClass(ClusterEvent.MemberUp.class);

      system.actorOf(
          ClusterSingletonManager.props(
              Master.props(workTimeout),
              PoisonPill.getInstance(),
              ClusterSingletonManagerSettings.create(system)
                  .withSingletonName("active")
          ),
          "master");

      Set<ActorPath> initialContacts = new HashSet<>();
      initialContacts.add(ActorPaths.fromString(clusterAddress + "/system/receptionist"));

      ActorRef clusterClient = system.actorOf(
        ClusterClient.props(ClusterClientSettings.create(system).withInitialContacts(initialContacts)),
        "clusterClient");


      for (int n = 1; n <= 3; n += 1) {
        system.actorOf(Worker.props(clusterClient,
          Props.create(WorkExecutor.class), registerInterval), "worker-" + n);
      }

      ActorRef flakyWorker = system.actorOf(Worker.props(clusterClient,
        Props.create(FlakyWorkExecutor.class), registerInterval), "flaky-worker");

      final ActorRef frontend = system.actorOf(Props.create(Frontend.class), "frontend");

      final JavaTestKit results = new JavaTestKit(system);

      DistributedPubSub.get(system).mediator().tell(
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
