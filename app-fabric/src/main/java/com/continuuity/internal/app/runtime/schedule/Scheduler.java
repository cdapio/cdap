package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.api.schedule.Schedule;
import com.continuuity.app.Id;
import com.continuuity.app.program.Type;

/**
 * Scheduler that schedules a program.
 */
public interface Scheduler {

  /**
   * Schedule a program to be run in a defined schedule.
   * @param program Program that needs to be run.
   * @param schedules Schedule with which the program runs.
   */
  public void schedule(Id.Program program, Type programType, Iterable<Schedule> schedules);

}
