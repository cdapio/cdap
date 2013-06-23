/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

/**
 * Enumerates class that defines policy for handling failure.
 */
public enum FailurePolicy {
  /**
   * Resend the input object to the process method again.
   */
  RETRY,

  /**
   * Skips the input object which will permanently remove it from the input.
   */
  IGNORE
}
