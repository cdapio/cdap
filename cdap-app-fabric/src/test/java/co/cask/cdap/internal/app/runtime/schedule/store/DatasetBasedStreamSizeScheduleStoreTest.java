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

package co.cask.cdap.internal.app.runtime.schedule.store;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.schedule.StreamSizeScheduleState;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class DatasetBasedStreamSizeScheduleStoreTest {

  public static DatasetBasedStreamSizeScheduleStore scheduleStore;

  private static final Id.Namespace NAMESPACE = new Id.Namespace(Constants.DEFAULT_NAMESPACE);
  private static final Id.Application APP_ID = new Id.Application(NAMESPACE, "AppWithStreamSizeSchedule");
  private static final Id.Program PROGRAM_ID = new Id.Program(APP_ID, ProgramType.WORKFLOW, "SampleWorkflow");
  private static final Id.Stream STREAM_ID = Id.Stream.from(NAMESPACE, "stream");
  private static final StreamSizeSchedule STREAM_SCHEDULE_1 = new StreamSizeSchedule("Schedule1", "Every 1M",
                                                                                     STREAM_ID.getName(), 1);
  private static final StreamSizeSchedule STREAM_SCHEDULE_2 = new StreamSizeSchedule("Schedule2", "Every 10M",
                                                                                     STREAM_ID.getName(), 10);
  private static final String SCHEDULE_NAME_1 = "Schedule1";
  private static final String SCHEDULE_NAME_2 = "Schedule2";
  private static final SchedulableProgramType PROGRAM_TYPE = SchedulableProgramType.WORKFLOW;

  @BeforeClass
  public static void set() throws Exception {
    scheduleStore = AppFabricTestHelper.getInjector().getInstance(DatasetBasedStreamSizeScheduleStore.class);
  }

  @Test
  public void testStreamSizeSchedule() throws Exception {
    scheduleStore.persist(PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_1, 0L, 0L, 0L, 0L, true);
    scheduleStore.persist(PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, 1000L, 10L, 1000L, 10L, false);

    // List all schedules
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_1, 0L, 0L, 0L, 0L, true
                          ),
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, 1000L, 10L, 1000L, 10L, false
                          )
                        ),
                        scheduleStore.list());

    // Suspend a schedule and check that this is reflected
    scheduleStore.suspend(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1);
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_1, 0L, 0L, 0L, 0L, false
                          ),
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, 1000L, 10L, 1000L, 10L, false
                          )
                        ),
                        scheduleStore.list());

    // Resume a schedule and check that this is reflected
    scheduleStore.resume(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2);
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_1, 0L, 0L, 0L, 0L, false
                          ),
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, 1000L, 10L, 1000L, 10L, true
                          )
                        ),
                        scheduleStore.list());

    // Update schedule base count info
    scheduleStore.updateBaseRun(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2, 10000L, 100L);
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_1, 0L, 0L, 0L, 0L, false
                          ),
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, 10000L, 100L, 1000L, 10L, true
                          )
                        ),
                        scheduleStore.list());

    // Update schedule last run info
    scheduleStore.updateLastRun(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1, 100L, 10000L, null);
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_1, 0L, 0L, 100L, 10000L, false
                          ),
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, 10000L, 100L, 1000L, 10L, true
                          )
                        ),
                        scheduleStore.list());

    // Update schedule object
    scheduleStore.updateSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1, STREAM_SCHEDULE_2);
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, 0L, 0L, 100L, 10000L, false
                          ),
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, 10000L, 100L, 1000L, 10L, true
                          )
                        ),
                        scheduleStore.list());

    // Delete schedules
    scheduleStore.delete(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1);
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, 10000L, 100L, 1000L, 10L, true
                          )
                        ),
                        scheduleStore.list());
    scheduleStore.delete(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2);
    Assert.assertEquals(ImmutableList.<StreamSizeScheduleState>of(),
                        scheduleStore.list());
  }
}
