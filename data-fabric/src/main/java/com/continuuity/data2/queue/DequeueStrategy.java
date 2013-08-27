/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

/**
 * Strategy for dequeue
 */
public enum DequeueStrategy {

  FIFO,
  ROUND_ROBIN,
  HASH
}
