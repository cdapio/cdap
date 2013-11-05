package com.continuuity.internal.schedule;

import com.continuuity.api.schedule.Schedule;

/**
 * Default schedule implementation.
 */
@Deprecated
public class DefaultSchedule extends Schedule {

  public DefaultSchedule(String name, String description, String cronEntry, Action action) {
    super(name, description, cronEntry, action);
  }
}
