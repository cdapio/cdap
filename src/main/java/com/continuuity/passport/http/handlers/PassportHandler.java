/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.handlers;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

/**
 *  Abstract class that holds metrics object that will exposed using JMX.
 */
public abstract class PassportHandler  {

  private final Counter requestsReceived = Metrics.newCounter(PassportHandler.class, "requests-received");
  private final Counter requestsSuccess = Metrics.newCounter(PassportHandler.class, "requests-success");
  private final Counter requestsFailed = Metrics.newCounter(PassportHandler.class, "requests-failed");

  public void requestReceived() {
    requestsReceived.inc();
  }

  public void requestSuccess() {
    requestsSuccess.inc();
  }

  public void requestFailed() {
    requestsFailed.inc();
  }


}
