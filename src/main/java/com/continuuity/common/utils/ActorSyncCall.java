package com.continuuity.common.utils;

import akka.actor.ActorRef;
import akka.dispatch.Await;
import akka.dispatch.Future;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ActorSyncCall facilitates making a synchronized call to an Actor. The generic
 * type T defines the return type returned by the Actor.
 *
 * <p>
 *   Here is an example of how ActorSyncCall is used:
 *   <pre>
 *     ActorSyncCall&lt;Integer&gt; actorSyncCall = new ActorSyncCall&lt;Integer&gt;(actorRef);
 *     Integer value = actorSyncCall.get(new ActorRequest(), timeout);
 *   </pre>
 * </p>
 */
public class ActorSyncCall<T> {
  private static final Logger Log = LoggerFactory.getLogger(ActorSyncCall.class);
  private ActorRef ref;

  /**
   * Initializes the instance of ActorSyncCall with the reference to the actor it needs to interact with.
   * @param ref to actor to which sync calls are made.
   */
  public ActorSyncCall(ActorRef ref) {
    this.ref = ref;
  }

  /**
   * Makes a sync (blocking) call passing in the <code>message</code> with a specified <code>timeout</code>
   * @param message to be passed to the actor.
   * @param timeout for sync call.
   * @return instance of type T
   * @throws ClassCastException
   */
  @SuppressWarnings("unchecked")
  public T get(Object message, Timeout timeout) throws ClassCastException {
    T object = null;
    try {
      Future<Object> future = Patterns.ask(ref, message, timeout);
      object = (T) Await.result(future, timeout.duration());
    } catch (Exception e) {
      Log.warn("Failed to get response from actor {} for message {}", new Object[]{ref.toString(), message});
    }
    return object;
  }
}

