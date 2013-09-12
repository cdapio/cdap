package com.continuuity.internal.schedule;

/**
 * Defines the schedule to run a program. Cron-based scheduling is supported for the schedule.
 */
public interface Schedule {

  /**
   * @return name of the schedule.
   */
  String getName();

  /**
   * @return schedule description.
   */
  String getDescription();

  /**
   * @return cronExpression for the schedule.
   */
  String getCronExpression();

  /**
   * @return Action for the schedule.
   */
  Action getAction();

  /**
   * Defines the ScheduleAction.
   */
  public enum Action {START, STOP};
}
