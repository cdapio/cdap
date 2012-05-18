package com.continuuity.common.conflake;

import akka.actor.ActorRef;

/**
 *
 */
class ConflakeRequest {
  private final int batchSize;
  private ActorRef ref;

  public ConflakeRequest(final int batchSize) {
    this.batchSize = batchSize;
    this.ref = null;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public ActorRef getActor() {
    return ref;
  }

  public void setActor(ActorRef ref) {
    this.ref = ref;
  }
}
