/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.metric;

import com.continuuity.overlord.metrics.client.Info;
import com.continuuity.overlord.metrics.client.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Immutable Integer counters.
 */
public class IntegerMetric extends AbstractMetric {
  private static final Logger LOG = LoggerFactory.getLogger(IntegerMetric.class);
  int value;

  /**
   * Constructor
   * @param info Name, description of counters
   * @param value Value of counters
   */
  public IntegerMetric(Info info, int value) {
    super(info);
    this.value = value;
  }

  /**
   * @return Returns the value of the counters
   */
  @Override
  public Integer getValue() {
    return value;
  }

  /**
   * @return  {@link Type} of counters
   */
  @Override
  public Type getType() {
    return Type.COUNTER;
  }

}
