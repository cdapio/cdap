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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.internal.app.runtime.schedule.trigger.AbstractCompositeTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.StreamSizeTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Abstract scheduler service common scheduling functionality. For each {@link Schedule} implementation, there is
 * a scheduler that this class will delegate the work to.
 * The extending classes should implement prestart and poststop hooks to perform any action before starting all
 * underlying schedulers and after stopping them.
 */
public abstract class AbstractSchedulerService extends AbstractIdleService implements SchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSchedulerService.class);
  private final TimeScheduler timeScheduler;
  private final StreamSizeScheduler streamSizeScheduler;
  private final Store store;

  public AbstractSchedulerService(TimeScheduler timeScheduler, StreamSizeScheduler streamSizeScheduler, Store store) {
    this.timeScheduler = timeScheduler;
    this.streamSizeScheduler = streamSizeScheduler;
    this.store = store;
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

    try {
      streamSizeScheduler.init();
      streamSizeScheduler.start();
      LOG.info("Started stream size scheduler");
    } catch (Throwable t) {
      Throwables.propagateIfPossible(t, SchedulerException.class);
      throw new SchedulerException(t);
    }
  }

  /**
   * Stop the quartz scheduler service.
   */
  protected final void stopScheduler() throws SchedulerException {
    try {
      streamSizeScheduler.stop();
      LOG.info("Stopped stream size scheduler");
    } catch (Throwable t) {
      LOG.error("Error stopping stream size scheduler", t);
      Throwables.propagateIfPossible(t, SchedulerException.class);
      throw new SchedulerException(t);
    } finally {
      try {
        timeScheduler.stop();
        LOG.info("Stopped time scheduler");
      } catch (Throwable t) {
        LOG.error("Error stopping time scheduler", t);
        Throwables.propagateIfPossible(t, SchedulerException.class);
        throw new SchedulerException(t);
      }
    }
  }

  @Override
  public void addProgramSchedule(ProgramSchedule schedule) throws AlreadyExistsException, SchedulerException {
    if (containsTimeTrigger(schedule)) {
      timeScheduler.addProgramSchedule(schedule);
    }
    if (containsStreamSizeTrigger(schedule)) {
      streamSizeScheduler.addProgramSchedule(schedule);
    }
  }

  @Override
  public void deleteProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {
    if (containsTimeTrigger(schedule)) {
      timeScheduler.deleteProgramSchedule(schedule);
    }
    if (containsStreamSizeTrigger(schedule)) {
      streamSizeScheduler.deleteProgramSchedule(schedule);
    }
  }

  @Override
  public void suspendProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {
    if (containsTimeTrigger(schedule)) {
      timeScheduler.suspendProgramSchedule(schedule);
    }
    if (containsStreamSizeTrigger(schedule)) {
      streamSizeScheduler.suspendProgramSchedule(schedule);
    }
  }

  @Override
  public void resumeProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {
    if (containsTimeTrigger(schedule)) {
      timeScheduler.resumeProgramSchedule(schedule);
    }
    if (containsStreamSizeTrigger(schedule)) {
      streamSizeScheduler.resumeProgramSchedule(schedule);
    }
  }

  private boolean containsTimeTrigger(ProgramSchedule schedule) {
    // A composite trigger may contain a TimeTrigger
    return schedule.getTrigger() instanceof TimeTrigger || schedule.getTrigger() instanceof AbstractCompositeTrigger;
  }

  private boolean containsStreamSizeTrigger(ProgramSchedule schedule) {
    // A composite trigger won't contain a StreamSizeTrigger since StreamSizeTrigger is no longer supported
    // in the new API introduced by 4.3
    return schedule.getTrigger() instanceof StreamSizeTrigger;
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

  @Override
  public ProgramScheduleStatus scheduleState(ProgramId program, SchedulableProgramType programType,
                                             String scheduleName)
    throws SchedulerException, NotFoundException {
    Scheduler scheduler = getSchedulerForSchedule(program, scheduleName);
    ProgramScheduleStatus status;
    try {
      status = scheduler.scheduleState(program, programType, scheduleName);
    } catch (NotFoundException e) {
      // Directly throw the NotFoundException if scheduler has already been started
      if (scheduler instanceof TimeScheduler) {
        if (((TimeScheduler) scheduler).isStarted()) {
          throw e;
        }
      } else {
        if (((StreamSizeScheduler) scheduler).isStarted()) {
          throw e;
        }
      }
      // Schedules may not be all read into memory before scheduler fully starts,
      // throw ServiceUnavailableException to retry next time
      throw new ServiceUnavailableException(this.getClass().getSimpleName());
    }
    return status;
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

  private Scheduler getSchedulerForSchedule(ProgramId program, String scheduleName) throws NotFoundException {
    ApplicationSpecification appSpec = store.getApplication(program.getParent());
    if (appSpec == null) {
      throw new ApplicationNotFoundException(program.getParent());
    }

    Map<String, ScheduleSpecification> schedules = appSpec.getSchedules();
    if (schedules == null || !schedules.containsKey(scheduleName)) {
      throw new ScheduleNotFoundException(program.getParent().schedule(scheduleName));
    }

    ScheduleSpecification scheduleSpec = schedules.get(scheduleName);
    Schedule schedule = scheduleSpec.getSchedule();
    return getScheduler(schedule);
  }

  private Scheduler getScheduler(Schedule schedule) {
    if (schedule instanceof TimeSchedule) {
      return timeScheduler;
    } else if (schedule instanceof StreamSizeSchedule) {
      return streamSizeScheduler;
    } else {
      throw new IllegalArgumentException("Unhandled type of schedule: " + schedule.getClass());
    }
  }
}
