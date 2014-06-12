/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

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
   * @return Number of attempts made to process the event, if {@link FailurePolicy} is set to RETRY else zero.
   */
  int getRetryCount();
}
