/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.counters;

import com.continuuity.overlord.metrics.client.Info;

/**
 * AbstractMutableCounter to abstract out some mundane stuff from
 * implementation.
 */
public abstract  class AbstractMutableCounter extends MutableCounter {
  private final Info info;
  
  protected AbstractMutableCounter(Info info) {
    this.info = info;
  }
  
  protected Info getInfo() {
    return info;
  }

  public abstract void increment();
}
