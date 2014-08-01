package worker;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.contrib.pattern.ClusterSingletonProxy;
import akka.dispatch.Mapper;
import akka.dispatch.Recover;
import akka.util.Timeout;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import static akka.pattern.Patterns.ask;
import static akka.pattern.Patterns.pipe;

public class Frontend extends UntypedActor {


  ActorRef masterProxy = getContext().actorOf(
      ClusterSingletonProxy.defaultProps("/user/master/active", "backend")
      , "masterProxy");

  public void onReceive(Object message) {

    Timeout timeout = new Timeout(5, TimeUnit.SECONDS);
    Future<Object> f = ask(masterProxy, message, timeout);


    final ExecutionContext ec = getContext().system().dispatcher();

    Future<Object> res = f.map(new Mapper<Object, Object>() {
      @Override
      public Object apply(Object msg) {
        if (msg instanceof Master.Ack)
          return Ok.getInstance();
        else
          return super.apply(msg);
      }
    }, ec).recover(new Recover<Object>() {
      @Override
      public Object recover(Throwable failure) throws Throwable {
        return NotOk.getInstance();
      }
    }, ec);

    pipe(res, ec).to(getSender());
  }

  public static final class Ok implements Serializable {
    private Ok() {}

    private static final Ok instance = new Ok();

    public static Ok getInstance() {
      return instance;
    }

    @Override
    public String toString() {
      return "Ok";
    }
  };

  public static final class NotOk implements Serializable {
    private NotOk() {}

    private static final NotOk instance = new NotOk();

    public static NotOk getInstance() {
      return instance;
    }

    @Override
    public String toString() {
      return "NotOk";
    }
  };
}
