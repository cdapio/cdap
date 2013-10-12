package com.continuuity.api.schedule;

/**
 * Defines a cron-based schedule for running a program. 
 */
public interface Schedule {

  /**
   * @return Name of the schedule.
   */
  String getName();

  /**
   * @return Schedule description.
   */
  String getDescription();

  /**
   * @return Cron expression for the schedule.
   */
  String getCronEntry();

  /**
   * @return Action for the schedule.
   */
  Action getAction();

  /**
   * Defines the ScheduleAction.
   */
  enum Action {START, STOP};
}
