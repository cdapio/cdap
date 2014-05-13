package com.continuuity.test;

/**
 * This interface is for managing the schedule of a workflow
 */
public interface ScheduleManager {
  /**
   * suspends the workflow schedule
   */
  public void suspend();

  /**
   * Resumes the workflow schedule
   */
  public void resume();

  /**
   * returns the status of the workflow schedule
   */
  public String status();
}
