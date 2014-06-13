package com.continuuity.api.schedule;

import com.continuuity.api.ProgramSpecification;

import java.util.List;

/**
 * Program specification used to define programs that can be scheduled to run periodically.
 */
public interface SchedulableProgramSpecification extends ProgramSpecification {

  /**
   * @return List of schedules.
   */
  List<Schedule> getSchedules();

}
