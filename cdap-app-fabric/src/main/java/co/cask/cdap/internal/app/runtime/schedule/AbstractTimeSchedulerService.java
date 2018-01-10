/*
 * Copyright © 2014-2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.app.runtime.schedule.trigger.AbstractSatisfiableCompositeTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Abstract scheduler service common scheduling functionality. Actual work will be delegated to {@link TimeScheduler}.
 * The extending classes should implement prestart and poststop hooks to perform any action before starting all
 * underlying schedulers and after stopping them.
 */
public abstract class AbstractTimeSchedulerService extends AbstractIdleService implements TimeSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTimeSchedulerService.class);
  private final TimeScheduler timeScheduler;

  public AbstractTimeSchedulerService(TimeScheduler timeScheduler) {
    this.timeScheduler = timeScheduler;
  }

  /**
   * Start the scheduler services, by initializing them and starting them
   */
  protected final void startSchedulers() throws SchedulerException {
    try {
      timeScheduler.init();
      timeScheduler.start();
      LOG.info("Started time scheduler");
    } catch (SchedulerException t) {
      Throwables.propagateIfPossible(t, SchedulerException.class);
      throw new SchedulerException(t);
    }
  }

  /**
   * Stop the quartz scheduler service.
   */
  protected final void stopScheduler() throws SchedulerException {
    try {
      timeScheduler.stop();
      LOG.info("Stopped time scheduler");
    } catch (Throwable t) {
      LOG.error("Error stopping time scheduler", t);
      Throwables.propagateIfPossible(t, SchedulerException.class);
      throw new SchedulerException(t);
    }
  }

  @Override
  public void addProgramSchedule(ProgramSchedule schedule) throws AlreadyExistsException, SchedulerException {
    if (containsTimeTrigger(schedule)) {
      timeScheduler.addProgramSchedule(schedule);
    }
  }

  @Override
  public void deleteProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {
    if (containsTimeTrigger(schedule)) {
      timeScheduler.deleteProgramSchedule(schedule);
    }
  }

  @Override
  public void suspendProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {
    if (containsTimeTrigger(schedule)) {
      timeScheduler.suspendProgramSchedule(schedule);
    }
  }

  @Override
  public void resumeProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {
    if (containsTimeTrigger(schedule)) {
      timeScheduler.resumeProgramSchedule(schedule);
    }
  }

  private boolean containsTimeTrigger(ProgramSchedule schedule) {
    // A composite trigger may contain a TimeTrigger
    return schedule.getTrigger() instanceof TimeTrigger
      || schedule.getTrigger() instanceof AbstractSatisfiableCompositeTrigger;
  }

  @Override
  public List<ScheduledRuntime> previousScheduledRuntime(ProgramId program, SchedulableProgramType programType)
    throws SchedulerException {
    return timeScheduler.previousScheduledRuntime(program, programType);
  }

  @Override
  public List<ScheduledRuntime> nextScheduledRuntime(ProgramId program, SchedulableProgramType programType)
    throws SchedulerException {
    return timeScheduler.nextScheduledRuntime(program, programType);
  }

  public static String scheduleIdFor(ProgramId program, SchedulableProgramType programType, String scheduleName) {
    return String.format("%s:%s", programIdFor(program, programType), scheduleName);
  }

  public static String getTriggerName(ProgramId program, SchedulableProgramType programType, String scheduleName,
                                      String cronEntry) {
    return String.format("%s:%s:%s", programIdFor(program, programType), scheduleName, cronEntry);
  }

  public static String programIdFor(ProgramId program, SchedulableProgramType programType) {
    return String.format("%s:%s:%s:%s:%s", program.getNamespace(), program.getApplication(), program.getVersion(),
                         programType.name(), program.getProgram());
  }
}
