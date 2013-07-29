/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.internal.io.Schema;

/**
 * This interface defines the specification for associated with either
 * input or output binds to it's respective {@link QueueName} and {@link Schema}
 */
public interface QueueSpecification {

  /**
   * @return {@link QueueName} associated with the queue.
   */
  QueueName getQueueName();

  /**
   * @return {@link Schema} associated with the reader schema of the queue.
   */
  Schema getInputSchema();

  /**
   * @return {@link Schema} associated with data written to the queue.
   */
  Schema getOutputSchema();
}
