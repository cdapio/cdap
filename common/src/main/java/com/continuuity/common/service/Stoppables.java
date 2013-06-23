package com.continuuity.common.service;

/**
 * Utility method for working with Stoppable objects.
 */
public final class Stoppables {

  /**
   * Quietly stops a service without throwing any exception.
   *
   * Current, {@code #stop} itself does not throw exception, but in future it can.
   * @param stopable The stopable to stop
   * @param reason   A message about why it was stopped.
   */
  public static void stopQuietly(Stoppable stopable, final String reason) {
    stopable.stop(reason);
  }

}