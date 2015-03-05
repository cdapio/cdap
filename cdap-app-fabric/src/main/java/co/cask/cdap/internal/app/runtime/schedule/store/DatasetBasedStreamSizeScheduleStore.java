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
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
  public void initialize() {
    try {
      table = tableUtil.getMetaTable();
      Preconditions.checkNotNull(table, "Could not get dataset client for data set: %s",
                                 ScheduleStoreTableUtil.SCHEDULE_STORE_DATASET_NAME);
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
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
                      long baseRunSize, long baseRunTs, long lastRunSize, long lastRunTs, boolean running) {
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
    updateTable(programId, programType, schedule.getName(), columns, values);
  }

  /**
   * Modify a schedule on the store to flag it as suspended.
   *
   * @param programId program id the schedule is running for
   * @param programType program type the schedule is running for
   * @param scheduleName name of the schedule
   */
  public void suspend(Id.Program programId, SchedulableProgramType programType, String scheduleName) {
    updateTable(programId, programType, scheduleName,
                new byte[][]{ ACTIVE_COL },
                new byte[][]{ Bytes.toBytes(false) });
  }

  /**
   * Modify a schedule on the store to flag it as running.
   *
   * @param programId program id the schedule is running for
   * @param programType program type the schedule is running for
   * @param scheduleName name of the schedule
   */
  public void resume(Id.Program programId, SchedulableProgramType programType, String scheduleName) {
    updateTable(programId, programType, scheduleName,
                new byte[][]{ ACTIVE_COL },
                new byte[][]{ Bytes.toBytes(true) });
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
                            long newBaseRunSize, long newBaseRunTs) {
    updateTable(programId, programType, scheduleName,
                new byte[][]{ BASE_SIZE_COL, BASE_TS_COL },
                new byte[][]{ Bytes.toBytes(newBaseRunSize), Bytes.toBytes(newBaseRunTs) });
  }

  /**
   * Modify the last run information of a schedule in the store.
   *
   * @param programId program id the schedule is running for
   * @param programType program type the schedule is running for
   * @param scheduleName name of the schedule
   * @param newLastRunSize new last run size
   * @param newLastRunTs new last run timestamp
   */
  public void updateLastRun(Id.Program programId, SchedulableProgramType programType,
                            String scheduleName, long newLastRunSize, long newLastRunTs) {
    updateTable(programId, programType, scheduleName,
                new byte[][]{ LAST_RUN_SIZE_COL, LAST_RUN_TS_COL },
                new byte[][]{ Bytes.toBytes(newLastRunSize), Bytes.toBytes(newLastRunTs) });
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
                             String scheduleName, StreamSizeSchedule newSchedule) {
    updateTable(programId, programType, scheduleName,
                new byte[][]{ SCHEDULE_COL },
                new byte[][]{ Bytes.toBytes(GSON.toJson(newSchedule)) });
  }

  /**
   * Remove a schedule from the store.
   *
   * @param programId program id the schedule is running for
   * @param programType program type the schedule is running for
   * @param scheduleName name of the schedule
   */
  public void delete(final Id.Program programId, final SchedulableProgramType programType, final String scheduleName) {
    try {
      factory.createExecutor(ImmutableList.of((TransactionAware) table))
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            byte[] rowKey = Bytes.toBytes(String.format("%s:%s", KEY_PREFIX,
                                                        getScheduleId(programId, programType, scheduleName)));
            table.delete(rowKey);
          }
        });
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
  }

  /**
   * @return a list of all the schedules and their states present in the store
   */
  public List<StreamSizeScheduleState> list() {
    final List<StreamSizeScheduleState> scheduleStates = Lists.newArrayList();
    try {
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
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
    return scheduleStates;
  }

  private void updateTable(final Id.Program programId, final SchedulableProgramType programType,
                           final String scheduleName, final byte[][] columns, final byte[][] values) {
    try {
      factory.createExecutor(ImmutableList.of((TransactionAware) table))
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            byte[] rowKey = Bytes.toBytes(String.format("%s:%s", KEY_PREFIX,
                                                        getScheduleId(programId, programType, scheduleName)));
            table.put(rowKey, columns, values);
            LOG.debug("Updated schedule {} with columns {}, values {}", scheduleName, columns, values);
          }
        });
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
  }

  private String getScheduleId(Id.Program program, SchedulableProgramType programType, String scheduleName) {
    return String.format("%s:%s", getProgramScheduleId(program, programType), scheduleName);
  }

  private String getProgramScheduleId(Id.Program program, SchedulableProgramType programType) {
    return String.format("%s:%s:%s:%s", program.getNamespaceId(), program.getApplicationId(),
                         programType.name(), program.getId());
  }

  /**
   * POJO containing a {@link StreamSizeSchedule} and its state.
   */
  public static class StreamSizeScheduleState {
    private final Id.Program programId;
    private final SchedulableProgramType programType;
    private final StreamSizeSchedule streamSizeSchedule;
    private final long baseRunSize;
    private final long baseRunTs;
    private final long lastRunSize;
    private final long lastRunTs;
    private final boolean running;

    @VisibleForTesting
    StreamSizeScheduleState(Id.Program programId, SchedulableProgramType programType,
                            StreamSizeSchedule streamSizeSchedule, long baseRunSize, long baseRunTs,
                            long lastRunSize, long lastRunTs, boolean running) {
      this.programId = programId;
      this.programType = programType;
      this.streamSizeSchedule = streamSizeSchedule;
      this.baseRunSize = baseRunSize;
      this.baseRunTs = baseRunTs;
      this.lastRunSize = lastRunSize;
      this.lastRunTs = lastRunTs;
      this.running = running;
    }

    public Id.Program getProgramId() {
      return programId;
    }

    public SchedulableProgramType getProgramType() {
      return programType;
    }

    public StreamSizeSchedule getStreamSizeSchedule() {
      return streamSizeSchedule;
    }

    public long getBaseRunSize() {
      return baseRunSize;
    }

    public long getBaseRunTs() {
      return baseRunTs;
    }

    public long getLastRunSize() {
      return lastRunSize;
    }

    public long getLastRunTs() {
      return lastRunTs;
    }

    public boolean isRunning() {
      return running;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("programId", programId)
        .add("programType", programType)
        .add("streamSizeSchedule", streamSizeSchedule)
        .add("baseRunSize", baseRunSize)
        .add("baseRunTs", baseRunTs)
        .add("lastRunSize", lastRunSize)
        .add("lastRunTs", lastRunTs)
        .add("running", running)
        .toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      StreamSizeScheduleState that = (StreamSizeScheduleState) o;
      return Objects.equal(programId, that.programId) &&
        Objects.equal(programType, that.programType) &&
        Objects.equal(streamSizeSchedule, that.streamSizeSchedule) &&
        Objects.equal(baseRunSize, that.baseRunSize) &&
        Objects.equal(baseRunTs, that.baseRunTs) &&
        Objects.equal(lastRunSize, that.lastRunSize) &&
        Objects.equal(lastRunTs, that.lastRunTs) &&
        Objects.equal(running, that.running);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(programId, programType, streamSizeSchedule, baseRunSize,
                              baseRunTs, lastRunSize, lastRunTs, running);
    }
  }

}