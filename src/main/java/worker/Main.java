package worker;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.contrib.pattern.ClusterClient;
import akka.contrib.pattern.ClusterSingletonManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashSet;
import java.util.Set;

public class Main {
  public static void main(String[] args) throws InterruptedException {
    Address joinAddress = startBackend(null, "backend");
    Thread.sleep(5000);
    startBackend(joinAddress, "backend");
    startWorker(joinAddress);
    Thread.sleep(5000);
    startFrontend(joinAddress);
  }

  private static String systemName = "Workers";
  private static FiniteDuration workTimeout = Duration.create(10, "seconds");

  public static Address startBackend(Address joinAddress, String role) {
    Config conf = ConfigFactory.parseString("akka.cluster.roles=[" + role + "]").
      withFallback(ConfigFactory.load());
    ActorSystem system = ActorSystem.create(systemName, conf);
    Address realJoinAddress =
      (joinAddress == null) ? Cluster.get(system).selfAddress() : joinAddress;
    Cluster.get(system).join(realJoinAddress);

    system.actorOf(ClusterSingletonManager.defaultProps(Master.props(workTimeout), "active",
      PoisonPill.getInstance(), role), "master");

    return realJoinAddress;
  }

  public static void startWorker(Address contactAddress) {
    ActorSystem system = ActorSystem.create(systemName);
    Set<ActorSelection> initialContacts = new HashSet<ActorSelection>();
    initialContacts.add(system.actorSelection(contactAddress + "/user/receptionist"));
    ActorRef clusterClient = system.actorOf(ClusterClient.defaultProps(initialContacts),
      "clusterClient");
    system.actorOf(Worker.props(clusterClient, Props.create(WorkExecutor.class)), "worker");
  }

  public static void startFrontend(Address joinAddress) {
    ActorSystem system = ActorSystem.create(systemName);
    Cluster.get(system).join(joinAddress);
    ActorRef frontend = system.actorOf(Props.create(Frontend.class), "frontend");
    system.actorOf(Props.create(WorkProducer.class, frontend), "producer");
    system.actorOf(Props.create(WorkResultConsumer.class), "consumer");
  }
}
