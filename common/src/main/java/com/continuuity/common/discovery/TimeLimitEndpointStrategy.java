/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.discovery;

import org.apache.twill.discovery.Discoverable;

import java.util.concurrent.TimeUnit;

/**
 * An {@link EndpointStrategy} that make sure it picks an endpoint within the given
 * timeout limit.
 */
public final class TimeLimitEndpointStrategy implements EndpointStrategy {

  private final EndpointStrategy delegate;
  private final long timeout;
  private final TimeUnit timeoutUnit;

  public TimeLimitEndpointStrategy(EndpointStrategy delegate, long timeout, TimeUnit timeoutUnit) {
    this.delegate = delegate;
    this.timeout = timeout;
    this.timeoutUnit = timeoutUnit;
  }

  @Override
  public Discoverable pick() {
    Discoverable pick = delegate.pick();
    try {
      long count = 0;
      while (pick == null && count++ < timeout) {
        timeoutUnit.sleep(1);
        pick = delegate.pick();
      }
    } catch (InterruptedException e) {
      // Simply propagate the interrupt.
      Thread.currentThread().interrupt();
    }
    return pick;
  }
}
