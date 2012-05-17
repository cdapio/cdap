/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.counters;

import com.continuuity.overlord.metrics.client.RecordBuilder;

/**
 * Abstract for implementation of Counters.
 */
public abstract class MutableCounter {
  public abstract void snapshot(RecordBuilder builder);
}
