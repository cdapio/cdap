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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.internal.app.runtime.schedule.AbstractSchedulerService;
import co.cask.cdap.internal.app.runtime.schedule.StreamSizeScheduleState;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Persists {@link StreamSizeSchedule} schedule information into datasets.
 */
@Singleton
public class DatasetBasedStreamSizeScheduleStore {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetBasedStreamSizeScheduleStore.class);

  private static final Gson GSON = new Gson();
  private static final String KEY_PREFIX = "streamSizeSchedule";
  private static final byte[] SCHEDULE_COL = Bytes.toBytes("schedule");
  private static final byte[] BASE_SIZE_COL = Bytes.toBytes("baseSize");
  private static final byte[] BASE_TS_COL = Bytes.toBytes("baseTs");
  private static final byte[] LAST_RUN_SIZE_COL = Bytes.toBytes("lastRunSize");
  private static final byte[] LAST_RUN_TS_COL = Bytes.toBytes("lastRunTs");
  private static final byte[] ACTIVE_COL = Bytes.toBytes("active");

  private final TransactionExecutorFactory factory;
  private final ScheduleStoreTableUtil tableUtil;
  private Table table;

  @Inject
  public DatasetBasedStreamSizeScheduleStore(TransactionExecutorFactory factory, ScheduleStoreTableUtil tableUtil) {
    this.tableUtil = tableUtil;
    this.factory = factory;
  }

  /**
   * Initialize this persistent store.
   */
  public void initialize() throws IOException, DatasetManagementException {
    table = tableUtil.getMetaTable();
    Preconditions.checkNotNull(table, "Could not get dataset client for data set: %s",
                               ScheduleStoreTableUtil.SCHEDULE_STORE_DATASET_NAME);
  }

  /**
   * Persist a schedule to the store.
   *
   * @param programId program id the schedule is running for
   * @param programType program type the schedule is running for
   * @param schedule the schedule itself
   * @param baseRunSize base size
   * @param baseRunTs base timestamp
   * @param running true if the schedule is running, false if it is suspended
   */
  public void persist(Id.Program programId, SchedulableProgramType programType, StreamSizeSchedule schedule,
                      long baseRunSize, long baseRunTs, long lastRunSize, long lastRunTs, boolean running)
    throws TransactionFailureException, InterruptedException {
    byte[][] columns = new byte[][] {
      SCHEDULE_COL, BASE_SIZE_COL, BASE_TS_COL, LAST_RUN_SIZE_COL, LAST_RUN_TS_COL, ACTIVE_COL
    };
    byte[][] values = new byte[][] {
      Bytes.toBytes(GSON.toJson(schedule)),
      Bytes.toBytes(baseRunSize),
      Bytes.toBytes(baseRunTs),
      Bytes.toBytes(lastRunSize),
      Bytes.toBytes(lastRunTs),
      Bytes.toBytes(running)
    };
    updateTable(programId, programType, schedule.getName(), columns, values, null);
  }

  /**
   * Modify a schedule on the store to flag it as suspended.
   *
   * @param programId program id the schedule is running for
   * @param programType program type the schedule is running for
   * @param scheduleName name of the schedule
   */
  public void suspend(Id.Program programId, SchedulableProgramType programType, String scheduleName)
    throws TransactionFailureException, InterruptedException {
    updateTable(programId, programType, scheduleName,
                new byte[][]{ ACTIVE_COL },
                new byte[][]{ Bytes.toBytes(false) },
                null);
  }

  /**
   * Modify a schedule on the store to flag it as running.
   *
   * @param programId program id the schedule is running for
   * @param programType program type the schedule is running for
   * @param scheduleName name of the schedule
   */
  public void resume(Id.Program programId, SchedulableProgramType programType, String scheduleName)
    throws TransactionFailureException, InterruptedException {
    updateTable(programId, programType, scheduleName,
                new byte[][]{ ACTIVE_COL },
                new byte[][]{ Bytes.toBytes(true) },
                null);
  }

  /**
   * Modify the base information of a schedule in the store.
   *
   * @param programId program id the schedule is running for
   * @param programType program type the schedule is running for
   * @param scheduleName name of the schedule
   * @param newBaseRunSize new base size
   * @param newBaseRunTs new base timestamp
   */
  public void updateBaseRun(Id.Program programId, SchedulableProgramType programType, String scheduleName,
                            long newBaseRunSize, long newBaseRunTs)
    throws TransactionFailureException, InterruptedException {
      updateTable(programId, programType, scheduleName,
                new byte[][]{ BASE_SIZE_COL, BASE_TS_COL },
                new byte[][]{ Bytes.toBytes(newBaseRunSize), Bytes.toBytes(newBaseRunTs) },
                null);
  }

  /**
   * Modify the last run information of a schedule in the store.
   *
   * @param programId program id the schedule is running for
   * @param programType program type the schedule is running for
   * @param scheduleName name of the schedule
   * @param newLastRunSize new last run size
   * @param newLastRunTs new last run timestamp
   * @param txMethod method to execute in the same transaction that will change the lastRun information,
   *                 before it is done
   */
  public void updateLastRun(Id.Program programId, SchedulableProgramType programType,
                            String scheduleName, long newLastRunSize, long newLastRunTs,
                            TransactionMethod txMethod)
    throws TransactionFailureException, InterruptedException {
    updateTable(programId, programType, scheduleName,
                new byte[][]{ LAST_RUN_SIZE_COL, LAST_RUN_TS_COL },
                new byte[][]{ Bytes.toBytes(newLastRunSize), Bytes.toBytes(newLastRunTs) },
                txMethod);
  }

  /**
   * Update the {@link StreamSizeSchedule} object for a schedule in the store.
   *
   * @param programId program id the schedule is running for
   * @param programType program type the schedule is running for
   * @param scheduleName name of the schedule
   * @param newSchedule new {@link StreamSizeSchedule} object
   */
  public void updateSchedule(Id.Program programId, SchedulableProgramType programType,
                             String scheduleName, StreamSizeSchedule newSchedule)
    throws TransactionFailureException, InterruptedException {
    updateTable(programId, programType, scheduleName,
                new byte[][]{ SCHEDULE_COL },
                new byte[][]{ Bytes.toBytes(GSON.toJson(newSchedule)) },
                null);
  }

  /**
   * Remove a schedule from the store.
   *
   * @param programId program id the schedule is running for
   * @param programType program type the schedule is running for
   * @param scheduleName name of the schedule
   */
  public void delete(final Id.Program programId, final SchedulableProgramType programType, final String scheduleName)
    throws InterruptedException, TransactionFailureException {
    factory.createExecutor(ImmutableList.of((TransactionAware) table))
      .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          byte[] rowKey = Bytes.toBytes(String.format("%s:%s", KEY_PREFIX,
                                                      AbstractSchedulerService.scheduleIdFor(programId, programType,
                                                                                             scheduleName)));
          table.delete(rowKey);
        }
      });
  }

  /**
   * @return a list of all the schedules and their states present in the store
   */
  public List<StreamSizeScheduleState> list() throws InterruptedException, TransactionFailureException {
    final List<StreamSizeScheduleState> scheduleStates = Lists.newArrayList();
    factory.createExecutor(ImmutableList.of((TransactionAware) table))
      .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          byte[] startKey = Bytes.toBytes(KEY_PREFIX);
          byte[] endKey = Bytes.stopKeyForPrefix(startKey);
          Scanner scan = table.scan(startKey, endKey);
          Row next;
          while ((next = scan.next()) != null) {
            byte[] scheduleBytes = next.get(SCHEDULE_COL);
            byte[] baseSizeBytes = next.get(BASE_SIZE_COL);
            byte[] baseTsBytes = next.get(BASE_TS_COL);
            byte[] lastRunSizeBytes = next.get(LAST_RUN_SIZE_COL);
            byte[] lastRunTsBytes = next.get(LAST_RUN_TS_COL);
            byte[] activeBytes = next.get(ACTIVE_COL);
            if (scheduleBytes == null || baseSizeBytes == null || baseTsBytes == null || lastRunSizeBytes == null ||
              lastRunTsBytes == null || activeBytes == null) {
              continue;
            }

            String rowKey = Bytes.toString(next.getRow());
            String[] splits = rowKey.split(":");
            if (splits.length != 6) {
              continue;
            }
            Id.Program program = Id.Program.from(splits[1], splits[2], ProgramType.valueOf(splits[3]), splits[4]);
            SchedulableProgramType programType = SchedulableProgramType.valueOf(splits[3]);

            StreamSizeSchedule schedule = GSON.fromJson(Bytes.toString(scheduleBytes), StreamSizeSchedule.class);
            long baseSize = Bytes.toLong(baseSizeBytes);
            long baseTs = Bytes.toLong(baseTsBytes);
            long lastRunSize = Bytes.toLong(lastRunSizeBytes);
            long lastRunTs = Bytes.toLong(lastRunTsBytes);
            boolean active = Bytes.toBoolean(activeBytes);
            StreamSizeScheduleState scheduleState =
              new StreamSizeScheduleState(program, programType, schedule, baseSize, baseTs,
                                          lastRunSize, lastRunTs, active);
            scheduleStates.add(scheduleState);
            LOG.debug("StreamSizeSchedule found in store: {}", scheduleState);
          }
        }
      });
    return scheduleStates;
  }

  private void updateTable(final Id.Program programId, final SchedulableProgramType programType,
                           final String scheduleName, final byte[][] columns, final byte[][] values,
                           @Nullable final TransactionMethod txMethod)
    throws InterruptedException, TransactionFailureException {
    factory.createExecutor(ImmutableList.of((TransactionAware) table))
      .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          if (txMethod != null) {
            txMethod.execute();
          }
          byte[] rowKey = Bytes.toBytes(String.format("%s:%s", KEY_PREFIX,
                                                      AbstractSchedulerService.scheduleIdFor(programId, programType,
                                                                                             scheduleName)));
          table.put(rowKey, columns, values);
          LOG.debug("Updated schedule {} with columns {}, values {}", scheduleName, columns, values);
        }
      });
  }

  /**
   * The {@link #execute} method of this interface is made to be run during a transaction.
   */
  public interface TransactionMethod {

    /**
     * Method to execute.
     *
     * @throws Exception
     */
    void execute() throws Exception;
  }
}
