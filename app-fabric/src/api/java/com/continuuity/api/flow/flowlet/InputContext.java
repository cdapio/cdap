/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

import com.continuuity.common.queue.QueueName;

/**
 * Represents the context of the input data that is passed
 * to {@link Flowlet} for processing.
 */
public interface InputContext {
  /**
   * @return Name of the output the event was read from.
   */
  String getOrigin();

  /**
   * @return {@link QueueName} of the origin if it is a queue, or null if it is not.
   */
  QueueName getOriginQueueName();

  /**
   * @return Number of attempts made to process the event, if {@link FailurePolicy} is set to RETRY else zero.
   */
  int getRetryCount();
}
