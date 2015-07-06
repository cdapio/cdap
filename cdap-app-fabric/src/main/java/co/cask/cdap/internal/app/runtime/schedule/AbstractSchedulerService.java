/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ScheduledRuntime;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
  private final CConfiguration cConf;
  private final Store store;

  public AbstractSchedulerService(TimeScheduler timeScheduler, StreamSizeScheduler streamSizeScheduler,
                                  CConfiguration cConf, Store store) {
    this.timeScheduler = timeScheduler;
    this.streamSizeScheduler = streamSizeScheduler;
    this.cConf = cConf;
    this.store = store;
  }

  private boolean isLazyStart() {
    return cConf.getBoolean(Constants.Scheduler.SCHEDULERS_LAZY_START, false);
  }

  /**
   * Start the scheduler services, by initializing them and starting them
   * right away if lazy start is not active.
   */
  protected final void startSchedulers() throws SchedulerException {
    try {
      timeScheduler.init();
      if (!isLazyStart()) {
        timeScheduler.lazyStart();
      }
      LOG.info("Started time scheduler");
    } catch (Throwable t) {
      Throwables.propagateIfInstanceOf(t, SchedulerException.class);
      throw new SchedulerException(t);
    }

    try {
      streamSizeScheduler.init();
      if (!isLazyStart()) {
        streamSizeScheduler.lazyStart();
      }
      LOG.info("Started stream size scheduler");
    } catch (Throwable t) {
      Throwables.propagateIfInstanceOf(t, SchedulerException.class);
      throw new SchedulerException(t);
    }
  }

  private void lazyStart(Scheduler scheduler) throws SchedulerException {
    if (scheduler instanceof TimeScheduler) {
      try {
        timeScheduler.lazyStart();
      } catch (Throwable t) {
        Throwables.propagateIfInstanceOf(t, SchedulerException.class);
        throw new SchedulerException(t);
      }
    } else if (scheduler instanceof StreamSizeScheduler) {
      try {
        streamSizeScheduler.lazyStart();
      } catch (Throwable t) {
        Throwables.propagateIfInstanceOf(t, SchedulerException.class);
        throw new SchedulerException(t);
      }
    }
  }

  private boolean isStarted(Scheduler scheduler) {
    if (scheduler instanceof TimeScheduler) {
      return ((TimeScheduler) scheduler).isStarted();
    } else if (scheduler instanceof StreamSizeScheduler) {
      return ((StreamSizeScheduler) scheduler).isStarted();
    }
    throw new IllegalArgumentException("Unrecognized type of scheduler for " + scheduler.getClass().toString());
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
      Throwables.propagateIfInstanceOf(t, SchedulerException.class);
      throw new SchedulerException(t);
    } finally {
      try {
        timeScheduler.stop();
        LOG.info("Stopped time scheduler");
      } catch (Throwable t) {
        LOG.error("Error stopping time scheduler", t);
        Throwables.propagateIfInstanceOf(t, SchedulerException.class);
        throw new SchedulerException(t);
      }
    }
  }

  @Override
  public void schedule(Id.Program programId, SchedulableProgramType programType, Schedule schedule)
    throws SchedulerException {
    schedule(programId, programType, schedule, ImmutableMap.<String, String>of());
  }

  @Override
  public void schedule(Id.Program programId, SchedulableProgramType programType, Schedule schedule,
                       Map<String, String> properties) throws SchedulerException {
    Scheduler scheduler;
    if (schedule instanceof TimeSchedule) {
      scheduler = timeScheduler;
    } else if (schedule instanceof StreamSizeSchedule) {
      scheduler = streamSizeScheduler;
    } else {
      throw new IllegalArgumentException("Unhandled type of schedule: " + schedule.getClass());
    }

    scheduler.schedule(programId, programType, schedule, properties);
    if (isLazyStart()) {
      // TODO: CDAP-2281 figure out a better way to handle schedules in unit tests
      String ignoreLazy = properties.get(Constants.Scheduler.IGNORE_LAZY_START);
      boolean shouldNotSuspend = ignoreLazy != null && Boolean.valueOf(ignoreLazy);
      if (shouldNotSuspend) {
        // normally in lazy mode, the scheduler is started on calls to resume.
        // If this schedule should be active right now instead of requiring a call to resume,
        // we need to start the scheduler here.
        lazyStart(scheduler);
      } else {
        try {
          scheduler.suspendSchedule(programId, programType, schedule.getName());
        } catch (NotFoundException e) {
          // Should not happen - we just created it. Could have been deleted just in between
          LOG.info("Schedule could not be suspended - it did not exist: {}", schedule.getName());
        }
      }
    }
  }

  @Override
  public void schedule(Id.Program programId, SchedulableProgramType programType, Iterable<Schedule> schedules)
    throws SchedulerException {
    schedule(programId, programType, schedules, ImmutableMap.<String, String>of());
  }

  @Override
  public void schedule(Id.Program programId, SchedulableProgramType programType, Iterable<Schedule> schedules,
                       Map<String, String> properties) throws SchedulerException {
    for (Schedule schedule : schedules) {
      schedule(programId, programType, schedule, properties);
    }
  }

  @Override
  public List<ScheduledRuntime> previousScheduledRuntime(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    return timeScheduler.previousScheduledRuntime(program, programType);
  }

  @Override
  public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    return timeScheduler.nextScheduledRuntime(program, programType);
  }

  @Override
  public List<String> getScheduleIds(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    return ImmutableList.<String>builder()
      .addAll(timeScheduler.getScheduleIds(program, programType))
      .addAll(streamSizeScheduler.getScheduleIds(program, programType))
      .build();
  }

  @Override
  public void suspendSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    Scheduler scheduler = getSchedulerForSchedule(program, scheduleName);
    scheduler.suspendSchedule(program, programType, scheduleName);
  }

  @Override
  public void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    Scheduler scheduler = getSchedulerForSchedule(program, scheduleName);
    if (!isStarted(scheduler) && isLazyStart()) {
      lazyStart(scheduler);
    }
    scheduler.resumeSchedule(program, programType, scheduleName);
  }

  @Override
  public void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws NotFoundException, SchedulerException {
    updateSchedule(program, programType, schedule, ImmutableMap.<String, String>of());
  }

  @Override
  public void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule,
                             Map<String, String> properties) throws NotFoundException, SchedulerException {
    Scheduler scheduler = getSchedulerForSchedule(program, schedule.getName());
    scheduler.updateSchedule(program, programType, schedule, properties);
  }

  @Override
  public void deleteSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    Scheduler scheduler = getSchedulerForSchedule(program, scheduleName);
    scheduler.deleteSchedule(program, programType, scheduleName);
  }

  @Override
  public void deleteSchedules(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    timeScheduler.deleteSchedules(program, programType);
    streamSizeScheduler.deleteSchedules(program, programType);
  }

  @Override
  public void deleteAllSchedules(Id.Namespace namespaceId) throws SchedulerException {
    for (ApplicationSpecification appSpec : store.getAllApplications(namespaceId)) {
      deleteAllSchedules(namespaceId, appSpec);
    }
  }

  private void deleteAllSchedules(Id.Namespace namespaceId, ApplicationSpecification appSpec)
    throws SchedulerException {
    for (ScheduleSpecification scheduleSpec : appSpec.getSchedules().values()) {
      Id.Application appId = Id.Application.from(namespaceId.getId(), appSpec.getName());
      ProgramType programType = ProgramType.valueOfSchedulableType(scheduleSpec.getProgram().getProgramType());
      Id.Program programId = Id.Program.from(appId, programType, scheduleSpec.getProgram().getProgramName());
      deleteSchedules(programId, scheduleSpec.getProgram().getProgramType());
    }
  }

  @Override
  public ScheduleState scheduleState(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws SchedulerException {
    try {
      Scheduler scheduler = getSchedulerForSchedule(program, scheduleName);
      return scheduler.scheduleState(program, programType, scheduleName);
    } catch (NotFoundException e) {
      return ScheduleState.NOT_FOUND;
    }
  }

  public static String scheduleIdFor(Id.Program program, SchedulableProgramType programType, String scheduleName) {
    return String.format("%s:%s", programIdFor(program, programType), scheduleName);
  }

  public static String programIdFor(Id.Program program, SchedulableProgramType programType) {
    return String.format("%s:%s:%s:%s", program.getNamespaceId(), program.getApplicationId(),
                         programType.name(), program.getId());
  }

  private Scheduler getSchedulerForSchedule(Id.Program program, String scheduleName) throws NotFoundException {
    ApplicationSpecification appSpec = store.getApplication(program.getApplication());
    if (appSpec == null) {
      throw new ApplicationNotFoundException(program.getApplication());
    }

    Map<String, ScheduleSpecification> schedules = appSpec.getSchedules();
    if (schedules == null || !schedules.containsKey(scheduleName)) {
      throw new ScheduleNotFoundException(Id.Schedule.from(program.getApplication(), scheduleName));
    }

    ScheduleSpecification scheduleSpec = schedules.get(scheduleName);
    Schedule schedule = scheduleSpec.getSchedule();
    if (schedule instanceof TimeSchedule) {
      return timeScheduler;
    } else if (schedule instanceof StreamSizeSchedule) {
      return streamSizeScheduler;
    }
    throw new IllegalArgumentException("Unhandled type of schedule: " + schedule.getClass());
  }
}
