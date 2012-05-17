/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.counters;

import com.continuuity.overlord.metrics.client.Info;
import com.continuuity.overlord.metrics.client.RecordBuilder;
import com.continuuity.overlord.metrics.client.annotation.Metric;

/**
 * Mutable Counter - A Integer counters which has max limit
 * of Integer.MAX_INT.
 */
public class Counter extends AbstractMutableCounter {
  private volatile  int value;
  private volatile int prev;
  private Metric.Type type;

  /**
   * Constructor
   * @param info Name, Description of counters
   * @param value  Value of counters
   */
  public Counter(Info info, Metric.Type type, int value) {
    super(info);
    this.value = value;
    this.type = type;
    this.prev = 0;
  }

  @Override
  public synchronized void increment() {
    ++value;
  }

  /**
   * Increments the counters by an amount
   * @param amount   Amount the counters should be incremented by
   */
  public synchronized void increment(int amount) {
    value+=amount;
  }

  /**
   * @return Value of the counters
   */
  public int getValue() {
    return value;
  }

  /**
   * Snapshot provides a snaphot of the counter. For cumulative counter the
   * snapshot returns the aggregate and for simple counter returns the value
   * collected since last snapshot.
   *
   * @param builder   Snapshot of the counters
   */
  @Override
  public void snapshot(RecordBuilder builder) {
    if(type.equals(Metric.Type.COUNTER))
      builder.addCounter(getInfo(), value-prev );
    else if(type.equals(Metric.Type.CUMULATIVE))
      builder.addCounter(getInfo(), value);
    prev = value;
  }
  
}
