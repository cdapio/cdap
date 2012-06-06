package com.continuuity.common.service;

/**
 * This is the interface that is used when the runners wants to support aborting.
 */
public interface Abortable {

  /**
   * Abortable service implements this method. Specifies reason and throwable.
   *
   * @param reason  for aborting.
   * @param throwable stack trace.
   */
  public void abort(String reason, Throwable throwable);

  /**
   * Checks if the service implementing this is aborted or no.
   *
   * @return true if aborted, false otherwise.
   */
  public boolean isAborted();
}
