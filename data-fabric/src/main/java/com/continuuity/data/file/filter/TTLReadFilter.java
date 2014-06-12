/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.file.filter;

import com.continuuity.data.file.ReadFilter;

/**
 * {@link com.continuuity.data.file.ReadFilter} for filtering expired stream events according to TTL and current time.
 */
public class TTLReadFilter extends ReadFilter {

  /**
   * Time to live.
   */
  private long ttl;

  public TTLReadFilter(long ttl) {
    this.ttl = ttl;
  }

  public long getTTL() {
    return ttl;
  }

  @Override
  public boolean acceptTimestamp(long timestamp) {
    return getCurrentTime() - timestamp <= ttl;
  }

  protected long getCurrentTime() {
    return System.currentTimeMillis();
  }
}
