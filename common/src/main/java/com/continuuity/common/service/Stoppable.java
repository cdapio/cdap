package com.continuuity.common.service;

/**
 * Interface implemented by classes that provide ability to
 * stop resources or subsystems that are started.
 */
public interface Stoppable {

  /**
   * Closes any resources held by the class implementing Stoppable.
   * If the resources and subsystems are already stopped then
   * invoking this has no effect.
   *
   * @param reason for stopping.
   */
  public void stop(final String reason);

  /**
   * Returns status about whether the thread was stopped.
   *
   * @return true if stopped; false otherwise.
   */
  public boolean isStopped();

}
