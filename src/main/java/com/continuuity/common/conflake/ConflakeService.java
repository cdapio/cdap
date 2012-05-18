package com.continuuity.common.conflake;

import akka.actor.*;
import akka.dispatch.Await;
import akka.dispatch.Future;
import akka.pattern.Patterns;
import akka.util.Duration;
import akka.util.Timeout;
import com.continuuity.common.utils.ActorSyncCall;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Actor based service of Conflake for generating sequence IDs.
 */
public class ConflakeService {
  private static final Logger Log = LoggerFactory.getLogger(ConflakeService.class);
  public static Conflake conflake = null;
  private static ActorSystem system = ActorSystem.create("conflake");
  private static ActorRef ref;


  /**
   * Starts the conflake service. It creates an implementation of ConflakeHandler.
   * @param zkEnsemble Specifies zookeeper.
   * @return true if successful; false otherwise.
   */
  public static synchronized boolean start(final String zkEnsemble) {
    boolean status = true;
    if(conflake == null) {
      try {
        conflake = new ConflakeHandler(zkEnsemble);
        ref = system.actorOf(new Props(new UntypedActorFactory() {
          @Override
          public Actor create() {
            return new ConflakeActor(conflake);
          }
        }), "conflake");
        conflake.start();
      } catch (Exception e) {
        Log.error("Failed starting the conflake. Reason : " + e.getMessage());
        status = false;
      }
    }
    return status;
  }

  /**
   * Returns an reference to Conflake actor.
   * @return ActorRef to conflake actor.
   */
  public static synchronized ImmutableList<Long> getIds(int batchSize) {
    Timeout timeout = new Timeout(Duration.create(2, TimeUnit.SECONDS));
    if(batchSize < 0) batchSize = 1;
    ActorSyncCall<ConflakeResponse> actorSyncCall = new ActorSyncCall<ConflakeResponse>(ref);
    ConflakeResponse response = actorSyncCall.get(new ConflakeRequest(batchSize), timeout);
    return response.getIds();
  }

  /**
   * Stop the Conflake Service
   */
  public static synchronized void stop() {
    try {
      if(ref.isTerminated()) {
        return;
      }
      Future<Boolean> stopped = Patterns.gracefulStop(ref, Duration.create(5, TimeUnit.SECONDS), system);
      Await.result(stopped, Duration.create(2, TimeUnit.SECONDS));
      conflake.stop();
    } catch (ActorTimeoutException e) {
      Log.error("Conflake service wasn't stopped within 5 seconds. Reason : " + e.getMessage());
    } catch (Exception e) {
      Log.error("Conflake service wasn't stopped within 5 second. Reason : " + e.getMessage());
    }
  }

}
