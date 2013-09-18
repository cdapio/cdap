package com.continuuity.internal.schedule;

import com.continuuity.api.schedule.Schedule;

/**
 * Default schedule implementation.
 */
public class DefaultSchedule implements Schedule {

  private final String name;
  private final String description;
  private final String cronEntry;
  private final Action action;

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String getCronEntry() {
    return cronEntry;
  }


  @Override
  public Action getAction() {
    return action;
  }

  public DefaultSchedule(String name, String description, String cronEntry, Action action) {
    this.name = name;
    this.description = description;
    this.cronEntry = cronEntry;
    this.action = action;
  }
}
