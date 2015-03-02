/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * {@link Scheduler} that triggers program executions based on data availability in streams.
 */
@Singleton
public class StreamSizeScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSizeScheduler.class);

  private final NotificationService notificationService;
  private final StreamAdmin streamAdmin;
  private final StoreFactory storeFactory;
  private final ProgramRuntimeService programRuntimeService;
  private final PreferencesStore preferencesStore;


  @Inject
  public StreamSizeScheduler(CConfiguration cConf, NotificationService notificationService, StreamAdmin streamAdmin,
                             StoreFactory storeFactory, ProgramRuntimeService programRuntimeService,
                             PreferencesStore preferencesStore) {
    this.notificationService = notificationService;
    this.streamAdmin = streamAdmin;
    this.storeFactory = storeFactory;
    this.programRuntimeService = programRuntimeService;
    this.preferencesStore = preferencesStore;
  }

  public void start() {

  }

  public void stop() {

  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws SchedulerException {
    Preconditions.checkArgument(schedule instanceof StreamSizeSchedule,
                                "Schedule should be of type StreamSizeSchedule");
  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Iterable<Schedule> schedules)
    throws SchedulerException {

  }

  @Override
  public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    return ImmutableList.of();
  }

  @Override
  public List<String> getScheduleIds(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    return ImmutableList.of();
  }

  @Override
  public void suspendSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {

  }

  @Override
  public void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {

  }

  @Override
  public void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws NotFoundException, SchedulerException {

  }

  @Override
  public void deleteSchedule(Id.Program programId, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {

  }

  @Override
  public void deleteSchedules(Id.Program programId, SchedulableProgramType programType)
    throws SchedulerException {

  }

  @Override
  public ScheduleState scheduleState(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws SchedulerException {
    return ScheduleState.NOT_FOUND;
  }

  private String getScheduleId(Id.Program program, SchedulableProgramType programType, String scheduleName) {
    return String.format("%s:%s", getProgramScheduleId(program, programType), scheduleName);
  }

  private String getProgramScheduleId(Id.Program program, SchedulableProgramType programType) {
    return String.format("%s:%s:%s:%s", program.getNamespaceId(), program.getApplicationId(),
                         programType.name(), program.getId());
  }
}
