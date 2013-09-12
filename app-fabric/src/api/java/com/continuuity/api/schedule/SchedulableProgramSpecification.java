package com.continuuity.api.schedule;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.internal.schedule.Schedule;

import java.util.List;

/**
 * Program specification that can be scheduled.
 */
public interface SchedulableProgramSpecification extends ProgramSpecification {

  /**
   * @return List of Schedule.
   */
  List<Schedule> getSchedules();

}
