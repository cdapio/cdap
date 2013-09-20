package com.continuuity.internal.app.runtime.schedule;

/**
 * Represents scheduled run time.
 */
public class ScheduledRuntime {

  private String scheduleId;
  private long time;

  public ScheduledRuntime(String scheduleId, long time) {
    this.scheduleId = scheduleId;
    this.time = time;
  }

  /**
   * @return schedule id.
   */
  public String getScheduleId() {
    return scheduleId;
  }

  /**
   * @return runtime.
   */
  public long getTime() {
    return time;
  }
}
