package com.continuuity.api.schedule;

/**
 * Defines a cron-based schedule for running a program. 
 */
public class Schedule {

  private final String name;

  private final String description;

  private final String cronEntry;

  private final Action action;

  public Schedule(String name, String description, String cronEntry, Action action) {
    this.name = name;
    this.description = description;
    this.cronEntry = cronEntry;
    this.action = action;
  }

  /**
   * @return Name of the schedule.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Schedule description.
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return Cron expression for the schedule.
   */
  public String getCronEntry() {
    return cronEntry;
  }

  /**
   * @return Action for the schedule.
   */
  public Action getAction() {
    return action;
  }

  /**
   * Defines the ScheduleAction.
   */
  public enum Action { START, STOP };
}
