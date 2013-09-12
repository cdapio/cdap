package com.continuuity.internal.schedule;

/**
 * Default schedule implementation.
 */
public class DefaultSchedule implements Schedule {

  private String name;
  private String description;
  private String cronExpression;
  private Action action;

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


  @Override
  public Action getAction() {
    return action;
  }

  public DefaultSchedule(String name, String description, String cronExpression, Action action) {
    this.name = name;
    this.description = description;
    this.cronExpression = cronExpression;
    this.action = action;
  }
}
