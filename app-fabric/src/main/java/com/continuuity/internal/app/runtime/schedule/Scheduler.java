package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.api.schedule.Schedule;
import com.continuuity.app.program.Program;

/**
 *
 */
public interface Scheduler {

  public void schedule(Program program, Schedule schedule);

  public void unSchedule(Program program);
}
