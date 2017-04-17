/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.runtime.schedule.StreamSizeScheduleState;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 *
 */
public class DatasetBasedStreamSizeScheduleStoreTest {

  public static DatasetBasedStreamSizeScheduleStore scheduleStore;
  public static TransactionExecutorFactory txExecutorFactory;
  public static DatasetFramework datasetFramework;

  private static final ApplicationId APP_ID = NamespaceId.DEFAULT.app("AppWithStreamSizeSchedule");
  private static final ProgramId PROGRAM_ID = APP_ID.program(ProgramType.WORKFLOW, "SampleWorkflow");

  private static final ApplicationId APP_ID_V1 = NamespaceId.DEFAULT.app("AppWithStreamSizeSchedule", "v1");
  private static final ProgramId PROGRAM_ID_V1 = APP_ID_V1.program(ProgramType.WORKFLOW, "SampleWorkflow");

  private static final Id.Stream STREAM_ID = Id.Stream.from(Id.Namespace.DEFAULT, "stream");
  private static final StreamSizeSchedule STREAM_SCHEDULE_1 = new StreamSizeSchedule("Schedule1", "Every 1M",
                                                                                     STREAM_ID.getId(), 1);
  private static final StreamSizeSchedule STREAM_SCHEDULE_2 = new StreamSizeSchedule("Schedule2", "Every 10M",
                                                                                     STREAM_ID.getId(), 10);
  private static final String SCHEDULE_NAME_1 = "Schedule1";
  private static final String SCHEDULE_NAME_2 = "Schedule2";
  private static final Map<String, String> MAP_1 = ImmutableMap.of("key1", "value1", "key2", "value2");
  private static final Map<String, String> MAP_2 = ImmutableMap.of("key3", "value3", "key4", "value4");
  private static final SchedulableProgramType PROGRAM_TYPE = SchedulableProgramType.WORKFLOW;

  @BeforeClass
  public static void set() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    scheduleStore = injector.getInstance(DatasetBasedStreamSizeScheduleStore.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
    datasetFramework = injector.getInstance(DatasetFramework.class);
  }

  @Test
  public void testOldDataFormatCompatibility() throws Exception {
    testDeletion(PROGRAM_ID);
    testDeletion(PROGRAM_ID_V1);
  }

  private void testDeletion(final ProgramId programId) throws Exception {
    final boolean defaultVersion = programId.getVersion().equals(ApplicationId.DEFAULT_VERSION);
    DatasetId storeTable = NamespaceId.SYSTEM.dataset(ScheduleStoreTableUtil.SCHEDULE_STORE_DATASET_NAME);
    final Table table = datasetFramework.getDataset(storeTable, ImmutableMap.<String, String>of(), null);
    Assert.assertNotNull(table);
    TransactionExecutor txnl = txExecutorFactory.createExecutor(ImmutableList.of((TransactionAware) table));
    final byte[] startKey = Bytes.toBytes(DatasetBasedStreamSizeScheduleStore.KEY_PREFIX);
    final byte[] stopKey = Bytes.stopKeyForPrefix(startKey);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Scanner scanner = table.scan(startKey, stopKey);
        Assert.assertNull(scanner.next());
        scanner.close();
      }
    });

    // Create one stream schedule - this will be persisted with new format
    scheduleStore.persist(programId, PROGRAM_TYPE, STREAM_SCHEDULE_1, MAP_1, 0L, 0L, 0L, 0L, true);

    // Create one stream schedule - based on the old format
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Create a programId without version so that we can create a old format schedule
        ProgramId defaultProgramId = new ProgramId(programId.getNamespace(), programId.getApplication(),
                                                   programId.getType(), programId.getProgram());
        String newRowKey = scheduleStore.getRowKey(defaultProgramId, PROGRAM_TYPE, STREAM_SCHEDULE_1.getName());
        Row row = table.get(Bytes.toBytes(scheduleStore.getRowKey(programId, PROGRAM_TYPE,
                                                                  STREAM_SCHEDULE_1.getName())));
        Assert.assertFalse(row.isEmpty());
        byte[] oldRowKey = Bytes.toBytes(scheduleStore.removeAppVersion(newRowKey));
        for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
          table.put(oldRowKey, entry.getKey(), entry.getValue());
        }
      }
    });

    // Make sure there are only two stream size schedules
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Scanner scanner = table.scan(startKey, stopKey);
        int numRows = 0;
        while (true) {
          Row row = scanner.next();
          if (row == null) {
            break;
          }
          numRows++;
        }
        scanner.close();
        Assert.assertEquals(2, numRows);
      }
    });

    // This delete should have deleted both the old and new row format
    scheduleStore.delete(programId, PROGRAM_TYPE, STREAM_SCHEDULE_1.getName());
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Scanner scanner = table.scan(startKey, stopKey);
        if (defaultVersion) {
          Assert.assertNull(scanner.next());
        } else {
          Assert.assertNotNull(scanner.next());
          Assert.assertNull(scanner.next());
        }
        scanner.close();
      }
    });

    // If the version is not default, we need to delete the row which didn't have a version
    if (!defaultVersion) {
      txnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // Create a programId without version so that we can create row key to delete the old format schedule
          ProgramId defaultProgramId = new ProgramId(programId.getNamespace(), programId.getApplication(),
                                                     programId.getType(), programId.getProgram());
          String newRowKey = scheduleStore.getRowKey(defaultProgramId, PROGRAM_TYPE, STREAM_SCHEDULE_1.getName());
          byte[] oldRowKey = Bytes.toBytes(scheduleStore.removeAppVersion(newRowKey));
          Row row = table.get(oldRowKey);
          Assert.assertFalse(row.isEmpty());
          table.delete(oldRowKey);
        }
      });
    }
  }

  @Test
  public void testStreamSizeSchedule() throws Exception {
    scheduleStore.persist(PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_1, MAP_1, 0L, 0L, 0L, 0L, true);
    scheduleStore.persist(PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, MAP_2, 1000L, 10L, 1000L, 10L, false);

    // List all schedules
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_1, MAP_1, 0L, 0L, 0L, 0L, true
                          ),
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, MAP_2, 1000L, 10L, 1000L, 10L, false
                          )
                        ),
                        scheduleStore.list());

    // Suspend a schedule and check that this is reflected
    scheduleStore.suspend(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1);
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_1, MAP_1, 0L, 0L, 0L, 0L, false
                          ),
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, MAP_2, 1000L, 10L, 1000L, 10L, false
                          )
                        ),
                        scheduleStore.list());

    // Resume a schedule and check that this is reflected
    scheduleStore.resume(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2);
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_1, MAP_1, 0L, 0L, 0L, 0L, false
                          ),
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, MAP_2, 1000L, 10L, 1000L, 10L, true
                          )
                        ),
                        scheduleStore.list());

    // Update schedule base count info
    scheduleStore.updateBaseRun(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2, 10000L, 100L);
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_1, MAP_1, 0L, 0L, 0L, 0L, false
                          ),
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, MAP_2, 10000L, 100L, 1000L, 10L, true
                          )
                        ),
                        scheduleStore.list());

    // Update schedule last run info
    scheduleStore.updateLastRun(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1, 100L, 10000L, null);
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_1, MAP_1, 0L, 0L, 100L, 10000L, false
                          ),
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, MAP_2, 10000L, 100L, 1000L, 10L, true
                          )
                        ),
                        scheduleStore.list());

    // Update schedule object
    scheduleStore.updateSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1, STREAM_SCHEDULE_2);
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, MAP_1, 0L, 0L, 100L, 10000L, false
                          ),
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, MAP_2, 10000L, 100L, 1000L, 10L, true
                          )
                        ),
                        scheduleStore.list());

    // Delete schedules
    scheduleStore.delete(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1);
    Assert.assertEquals(ImmutableList.of(
                          new StreamSizeScheduleState(
                            PROGRAM_ID, PROGRAM_TYPE, STREAM_SCHEDULE_2, MAP_2, 10000L, 100L, 1000L, 10L, true
                          )
                        ),
                        scheduleStore.list());
    scheduleStore.delete(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2);
    Assert.assertEquals(ImmutableList.<StreamSizeScheduleState>of(),
                        scheduleStore.list());
  }
}
