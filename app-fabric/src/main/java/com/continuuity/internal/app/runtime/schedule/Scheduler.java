package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.api.schedule.Schedule;
import com.continuuity.app.program.Program;

/**
 * Scheduler that schedules a program.
 */
public interface Scheduler {

  /**
   * Schedule a program to be run in a defined schedule.
   * @param program Program that needs to be run.
   * @param schedule Schedule with which the program runs.
   */
  public void schedule(Program program, Schedule schedule);

}
