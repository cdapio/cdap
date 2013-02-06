package com.continuuity.api.flow.flowlet;

/**
 * Enumerates class that defines multiple ways of handling a failure policy.
 */
public enum FailurePolicy {
  RETRY, // In case of failure, the event is tried again.
  IGNORE // In case of failure, the event is ignore from reprocessing.
}
