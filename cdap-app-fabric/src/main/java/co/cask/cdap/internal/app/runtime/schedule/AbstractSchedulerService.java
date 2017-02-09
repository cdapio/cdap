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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
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
  public void schedule(ProgramId programId, SchedulableProgramType programType, Schedule schedule)
    throws SchedulerException {
    schedule(programId, programType, schedule, ImmutableMap.<String, String>of());
  }

  @Override
  public void schedule(ProgramId programId, SchedulableProgramType programType, Schedule schedule,
                       Map<String, String> properties) throws SchedulerException {
    Scheduler scheduler;
    scheduler = getScheduler(schedule);

    scheduler.schedule(programId, programType, schedule, properties);
  }

  @Override
  public void schedule(ProgramId programId, SchedulableProgramType programType, Iterable<Schedule> schedules)
    throws SchedulerException {
    schedule(programId, programType, schedules, ImmutableMap.<String, String>of());
  }

  @Override
  public void schedule(ProgramId programId, SchedulableProgramType programType, Iterable<Schedule> schedules,
                       Map<String, String> properties) throws SchedulerException {
    for (Schedule schedule : schedules) {
      schedule(programId, programType, schedule, properties);
    }
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
  public List<String> getScheduleIds(ProgramId program, SchedulableProgramType programType)
    throws SchedulerException {
    return ImmutableList.<String>builder()
      .addAll(timeScheduler.getScheduleIds(program, programType))
      .addAll(streamSizeScheduler.getScheduleIds(program, programType))
      .build();
  }

  @Override
  public void suspendSchedule(ProgramId program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    Scheduler scheduler = getSchedulerForSchedule(program, scheduleName);
    scheduler.suspendSchedule(program, programType, scheduleName);
  }

  @Override
  public void resumeSchedule(ProgramId program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    Scheduler scheduler = getSchedulerForSchedule(program, scheduleName);
    scheduler.resumeSchedule(program, programType, scheduleName);
  }

  @Override
  public void updateSchedule(ProgramId program, SchedulableProgramType programType, Schedule schedule)
    throws NotFoundException, SchedulerException {
    updateSchedule(program, programType, schedule, ImmutableMap.<String, String>of());
  }

  @Override
  public void updateSchedule(ProgramId program, SchedulableProgramType programType, Schedule schedule,
                             Map<String, String> properties) throws NotFoundException, SchedulerException {
    Scheduler scheduler = getSchedulerForSchedule(program, schedule.getName());
    ScheduleState scheduleState = scheduleState(program, programType, schedule.getName());
    scheduler.updateSchedule(program, programType, schedule, properties);
    // the update of schedule will delete and a create new one so we have to suspend it if it was suspended
    if (scheduleState == ScheduleState.SUSPENDED) {
      suspendSchedule(program, programType, schedule.getName());
    }
  }

  @Override
  public void deleteSchedule(ProgramId program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    Scheduler scheduler = getSchedulerForSchedule(program, scheduleName);
    scheduler.deleteSchedule(program, programType, scheduleName);
  }

  @Override
  public void deleteSchedules(ProgramId program, SchedulableProgramType programType)
    throws SchedulerException {
    timeScheduler.deleteSchedules(program, programType);
    streamSizeScheduler.deleteSchedules(program, programType);
  }

  @Override
  public void deleteAllSchedules(NamespaceId namespaceId) throws SchedulerException {
    for (ApplicationSpecification appSpec : store.getAllApplications(namespaceId)) {
      deleteAllSchedules(namespaceId, appSpec);
    }
  }

  private void deleteAllSchedules(NamespaceId namespaceId, ApplicationSpecification appSpec)
    throws SchedulerException {
    for (ScheduleSpecification scheduleSpec : appSpec.getSchedules().values()) {
      ApplicationId appId = namespaceId.app(appSpec.getName(), appSpec.getAppVersion());
      ProgramType programType = ProgramType.valueOfSchedulableType(scheduleSpec.getProgram().getProgramType());
      ProgramId programId = appId.program(programType, scheduleSpec.getProgram().getProgramName());
      deleteSchedules(programId, scheduleSpec.getProgram().getProgramType());
    }
  }

  @Override
  public ScheduleState scheduleState(ProgramId program, SchedulableProgramType programType, String scheduleName)
    throws SchedulerException {
    try {
      Scheduler scheduler = getSchedulerForSchedule(program, scheduleName);
      return scheduler.scheduleState(program, programType, scheduleName);
    } catch (NotFoundException e) {
      return ScheduleState.NOT_FOUND;
    }
  }

  public static String scheduleIdFor(ProgramId program, SchedulableProgramType programType, String scheduleName) {
    return String.format("%s:%s", programIdFor(program, programType), scheduleName);
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
