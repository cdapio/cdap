package com.continuuity.internal.schedule;

/**
 * Default schedule implementation.
 */
public class DefaultSchedule implements Schedule {

  private String name;
  private String description;
  private String cronExpression;

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String getCronExpression() {
    return cronExpression;
  }

  public DefaultSchedule(String name, String description, String cronExpression) {
    this.name = name;
    this.description = description;
    this.cronExpression = cronExpression;
  }
}
