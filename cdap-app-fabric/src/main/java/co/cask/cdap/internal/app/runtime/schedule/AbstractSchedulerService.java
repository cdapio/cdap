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
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.exception.ApplicationNotFoundException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.proto.Id;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
  private final StoreFactory storeFactory;

  private Store store;

  public AbstractSchedulerService(Supplier<org.quartz.Scheduler> schedulerSupplier,
                                  StreamSizeScheduler streamSizeScheduler,
                                  StoreFactory storeFactory, ProgramRuntimeService programRuntimeService,
                                  PreferencesStore preferencesStore) {
    this.timeScheduler = new TimeScheduler(schedulerSupplier, storeFactory, programRuntimeService, preferencesStore);
    this.streamSizeScheduler = streamSizeScheduler;
    this.storeFactory = storeFactory;
  }

  /**
   * Start the quartz scheduler service.
   */
  protected final void startScheduler() throws SchedulerException {
    try {
      timeScheduler.start();
      LOG.info("Started time scheduler");
    } catch (Throwable t) {
      LOG.error("Error starting time scheduler", t);
      Throwables.propagateIfInstanceOf(t, SchedulerException.class);
      throw new SchedulerException(t);
    }

    try {
      streamSizeScheduler.start();
      LOG.info("Started stream size scheduler");
    } catch (Throwable t) {
      LOG.error("Error starting stream size scheduler", t);
      Throwables.propagateIfInstanceOf(t, SchedulerException.class);
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
    if (schedule instanceof TimeSchedule) {
      timeScheduler.schedule(programId, programType, schedule);
    } else if (schedule instanceof StreamSizeSchedule) {
      streamSizeScheduler.schedule(programId, programType, schedule);
    } else {
      throw new IllegalArgumentException("Unhandled type of schedule: " + schedule.getClass());
    }
  }

  @Override
  public void schedule(Id.Program programId, SchedulableProgramType programType, Iterable<Schedule> schedules)
    throws SchedulerException {
    Set<Schedule> timeSchedules = Sets.newHashSet();
    Set<Schedule> streamSizeSchedules = Sets.newHashSet();
    for (Schedule schedule : schedules) {
      if (schedule instanceof TimeSchedule) {
        timeSchedules.add(schedule);
      } else if (schedule instanceof StreamSizeSchedule) {
        streamSizeSchedules.add(schedule);
      } else {
        throw new IllegalArgumentException("Unhandled type of schedule: " + schedule.getClass());
      }
    }
    if (!timeSchedules.isEmpty()) {
      timeScheduler.schedule(programId, programType, timeSchedules);
    }
    if (!streamSizeSchedules.isEmpty()) {
      streamSizeScheduler.schedule(programId, programType, streamSizeSchedules);
    }
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
    Scheduler scheduler = getSchedulerForSchedule(program, programType, scheduleName);
    scheduler.suspendSchedule(program, programType, scheduleName);
  }

  @Override
  public void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    Scheduler scheduler = getSchedulerForSchedule(program, programType, scheduleName);
    scheduler.resumeSchedule(program, programType, scheduleName);
  }

  @Override
  public void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws NotFoundException, SchedulerException {
    Scheduler scheduler = getSchedulerForSchedule(program, programType, schedule.getName());
    scheduler.updateSchedule(program, programType, schedule);
  }

  @Override
  public void deleteSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    Scheduler scheduler = getSchedulerForSchedule(program, programType, scheduleName);
    scheduler.deleteSchedule(program, programType, scheduleName);
  }

  @Override
  public void deleteSchedules(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    timeScheduler.deleteSchedules(program, programType);
    streamSizeScheduler.deleteSchedules(program, programType);
  }

  @Override
  public ScheduleState scheduleState(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws SchedulerException {
    try {
      Scheduler scheduler = getSchedulerForSchedule(program, programType, scheduleName);
      return scheduler.scheduleState(program, programType, scheduleName);
    } catch (NotFoundException e) {
      return ScheduleState.NOT_FOUND;
    }
  }

  protected static String scheduleIdFor(Id.Program program, SchedulableProgramType programType, String scheduleName) {
    return String.format("%s:%s", programIdFor(program, programType), scheduleName);
  }

  protected static String programIdFor(Id.Program program, SchedulableProgramType programType) {
    return String.format("%s:%s:%s:%s", program.getNamespaceId(), program.getApplicationId(),
                         programType.name(), program.getId());
  }

  private synchronized Store getStore() {
    if (store == null) {
      store = storeFactory.create();
    }
    return store;
  }

  private Scheduler getSchedulerForSchedule(Id.Program program, SchedulableProgramType programType,
                                            String scheduleName) throws NotFoundException {
    ApplicationSpecification appSpec = getStore().getApplication(program.getApplication());
    if (appSpec == null) {
      throw new ApplicationNotFoundException(program.getApplication());
    }

    Map<String, ScheduleSpecification> schedules = appSpec.getSchedules();
    Id.Schedule scheduleId = Id.Schedule.from(program, programType, scheduleName);
    if (schedules == null || !schedules.containsKey(scheduleName)) {
      throw new ScheduleNotFoundException(scheduleId);
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
