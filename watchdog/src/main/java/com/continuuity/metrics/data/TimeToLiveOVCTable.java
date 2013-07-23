/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

/**
 * Interface to indicate TTL is supported by the table.
 */
public interface TimeToLiveOVCTable {

  /**
   * Returns the current TTL setting of the table.
   * @return The TTL or -1 if no TTL is set.
   */
  int getTTL();
}
