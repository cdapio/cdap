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
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.internal.app.runtime.schedule.AbstractSchedulerService;
import co.cask.cdap.internal.app.runtime.schedule.StreamSizeScheduleState;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionNotInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Persists {@link StreamSizeSchedule} schedule information into datasets.
 */
@Singleton
public class DatasetBasedStreamSizeScheduleStore {
  public static final String KEY_PREFIX = "streamSizeSchedule";
  private static final String NAME = "StreamSizeScheduleStore";

  private static final Logger LOG = LoggerFactory.getLogger(DatasetBasedStreamSizeScheduleStore.class);
  // Limit the number of logs when the rows have invalid columns. This will be called only during the startup when the
  // scheduler is initialized from the store entries and since we don't expect to have many data sized schedules,
  // we can log once every N log calls
  private static final Logger LIMITED_LOG = Loggers.sampling(LOG, LogSamplers.onceEvery(100));

  private static final Gson GSON = new Gson();
  private static final byte[] APP_VERSION_UPGRADE_KEY = Bytes.toBytes("version.streamsize.schedule");
  private static final byte[] COLUMN = Bytes.toBytes('c');
  private static final byte[] SCHEDULE_COL = Bytes.toBytes("schedule");
  private static final byte[] BASE_SIZE_COL = Bytes.toBytes("baseSize");
  private static final byte[] BASE_TS_COL = Bytes.toBytes("baseTs");
  private static final byte[] LAST_RUN_SIZE_COL = Bytes.toBytes("lastRunSize");
  private static final byte[] LAST_RUN_TS_COL = Bytes.toBytes("lastRunTs");
  private static final byte[] ACTIVE_COL = Bytes.toBytes("active");
  private static final byte[] PROPERTIES_COL = Bytes.toBytes("properties");
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final TransactionExecutorFactory factory;
  private final ScheduleStoreTableUtil tableUtil;
  private final AtomicBoolean storeInitialized;

  private LoadingCache<byte[], Boolean> upgradeCacheLoader;
  private Table table;

  @Inject
  public DatasetBasedStreamSizeScheduleStore(TransactionExecutorFactory factory, ScheduleStoreTableUtil tableUtil) {
    this.tableUtil = tableUtil;
    this.factory = factory;
    this.storeInitialized = new AtomicBoolean(false);
  }

  /**
   * Initialize this persistent store.
   */
  public synchronized void initialize() throws IOException, DatasetManagementException {
    table = tableUtil.getMetaTable();
    Preconditions.checkNotNull(table, "Could not get dataset client for data set: %s",
                               ScheduleStoreTableUtil.SCHEDULE_STORE_DATASET_NAME);
    upgradeCacheLoader = CacheBuilder.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      // Use a new instance of table since Table is not thread safe
      .build(new UpgradeValueLoader(NAME, factory, tableUtil.getMetaTable(), storeInitialized));
    storeInitialized.set(true);
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
  public void persist(ProgramId programId, SchedulableProgramType programType, StreamSizeSchedule schedule,
                      Map<String, String> properties, long baseRunSize, long baseRunTs, long lastRunSize,
                      long lastRunTs, boolean running)
    throws TransactionFailureException, InterruptedException {
    byte[][] columns = new byte[][] {
      SCHEDULE_COL, BASE_SIZE_COL, BASE_TS_COL, LAST_RUN_SIZE_COL, LAST_RUN_TS_COL, ACTIVE_COL, PROPERTIES_COL
    };
    byte[][] values = new byte[][] {
      Bytes.toBytes(GSON.toJson(schedule)),
      Bytes.toBytes(baseRunSize),
      Bytes.toBytes(baseRunTs),
      Bytes.toBytes(lastRunSize),
      Bytes.toBytes(lastRunTs),
      Bytes.toBytes(running),
      Bytes.toBytes(GSON.toJson(properties))
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
  public void suspend(ProgramId programId, SchedulableProgramType programType, String scheduleName)
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
  public void resume(ProgramId programId, SchedulableProgramType programType, String scheduleName)
    throws TransactionFailureException, InterruptedException {
    updateTable(programId, programType, scheduleName,
                new byte[][]{ACTIVE_COL},
                new byte[][]{Bytes.toBytes(true)},
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
  public void updateBaseRun(ProgramId programId, SchedulableProgramType programType, String scheduleName,
                            long newBaseRunSize, long newBaseRunTs)
    throws TransactionFailureException, InterruptedException {
      updateTable(programId, programType, scheduleName,
                  new byte[][]{BASE_SIZE_COL, BASE_TS_COL},
                  new byte[][]{Bytes.toBytes(newBaseRunSize), Bytes.toBytes(newBaseRunTs)},
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
  public void updateLastRun(ProgramId programId, SchedulableProgramType programType,
                            String scheduleName, long newLastRunSize, long newLastRunTs,
                            TransactionMethod txMethod)
    throws TransactionFailureException, InterruptedException {
    updateTable(programId, programType, scheduleName,
                new byte[][]{LAST_RUN_SIZE_COL, LAST_RUN_TS_COL},
                new byte[][]{Bytes.toBytes(newLastRunSize), Bytes.toBytes(newLastRunTs)},
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
  public void updateSchedule(ProgramId programId, SchedulableProgramType programType,
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
  public synchronized void delete(final ProgramId programId, final SchedulableProgramType programType,
                                  final String scheduleName) throws InterruptedException, TransactionFailureException {
    final boolean needVersionLessDelete = !isUpgradeComplete();
    factory.createExecutor(ImmutableList.of((TransactionAware) table))
      .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          String rowKey = getRowKey(programId, programType, scheduleName);
          if (needVersionLessDelete) {
            String versionLessRowKey = removeAppVersion(rowKey);
            if (versionLessRowKey != null) {
              table.delete(Bytes.toBytes(versionLessRowKey));
            }
          }
          table.delete(Bytes.toBytes(rowKey));
        }
      });
  }

  @VisibleForTesting
  String getRowKey(ProgramId programId, SchedulableProgramType programType, String scheduleName) {
    return String.format("%s:%s", KEY_PREFIX, AbstractSchedulerService.scheduleIdFor(
      programId, programType, scheduleName));
  }

  /**
   * @return a list of all the schedules and their states present in the store
   */
  public synchronized List<StreamSizeScheduleState> list() throws InterruptedException, TransactionFailureException {
    final List<StreamSizeScheduleState> scheduleStates = Lists.newArrayList();
    factory.createExecutor(ImmutableList.of((TransactionAware) table))
      .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          try (Scanner scan = getScannerWithPrefix(table, KEY_PREFIX)) {
            Row row;
            while ((row = scan.next()) != null) {
              byte[] scheduleBytes = row.get(SCHEDULE_COL);
              byte[] baseSizeBytes = row.get(BASE_SIZE_COL);
              byte[] baseTsBytes = row.get(BASE_TS_COL);
              byte[] lastRunSizeBytes = row.get(LAST_RUN_SIZE_COL);
              byte[] lastRunTsBytes = row.get(LAST_RUN_TS_COL);
              byte[] activeBytes = row.get(ACTIVE_COL);
              byte[] propertyBytes = row.get(PROPERTIES_COL);
              if (isInvalidRow(row)) {
                LIMITED_LOG.debug("Stream Sized Schedule entry with Row key {} does not have all columns.",
                                  Bytes.toString(row.getRow()));
                continue;
              }

              String rowKey = Bytes.toString(row.getRow());
              String[] splits = rowKey.split(":");
              ProgramId program;

              if (splits.length == 7) {
                // New Row key for the trigger should be of the form -
                // streamSizeSchedule:namespace:application:version:type:program:schedule
                program = new ApplicationId(splits[1], splits[2], splits[3])
                  .program(ProgramType.valueOf(splits[4]), splits[5]);
              } else if (splits.length == 6) {
                program = new ApplicationId(splits[1], splits[2]).program(ProgramType.valueOf(splits[3]), splits[4]);
              } else {
                continue;
              }

              SchedulableProgramType programType = program.getType().getSchedulableType();
              StreamSizeSchedule schedule = GSON.fromJson(Bytes.toString(scheduleBytes), StreamSizeSchedule.class);
              long baseSize = Bytes.toLong(baseSizeBytes);
              long baseTs = Bytes.toLong(baseTsBytes);
              long lastRunSize = Bytes.toLong(lastRunSizeBytes);
              long lastRunTs = Bytes.toLong(lastRunTsBytes);
              boolean active = Bytes.toBoolean(activeBytes);
              Map<String, String> properties = Maps.newHashMap();
              if (propertyBytes != null) {
                properties = GSON.fromJson(Bytes.toString(propertyBytes), STRING_MAP_TYPE);
              }
              StreamSizeScheduleState scheduleState =
                new StreamSizeScheduleState(program, programType, schedule, properties, baseSize, baseTs,
                                            lastRunSize, lastRunTs, active);
              scheduleStates.add(scheduleState);
              LOG.debug("StreamSizeSchedule found in store: {}", scheduleState);
            }
          }
        }
      });
    return scheduleStates;
  }

  private Scanner getScannerWithPrefix(Table table, String keyPrefix) {
    byte[] startKey = Bytes.toBytes(keyPrefix);
    byte[] endKey = Bytes.stopKeyForPrefix(startKey);
    return table.scan(startKey, endKey);
  }

  private boolean isInvalidRow(Row row) {
    byte[] scheduleBytes = row.get(SCHEDULE_COL);
    byte[] baseSizeBytes = row.get(BASE_SIZE_COL);
    byte[] baseTsBytes = row.get(BASE_TS_COL);
    byte[] lastRunSizeBytes = row.get(LAST_RUN_SIZE_COL);
    byte[] lastRunTsBytes = row.get(LAST_RUN_TS_COL);
    byte[] activeBytes = row.get(ACTIVE_COL);
    return scheduleBytes == null || baseSizeBytes == null || baseTsBytes == null || lastRunSizeBytes == null ||
      lastRunTsBytes == null || activeBytes == null;
  }

  private synchronized void updateTable(final ProgramId programId, final SchedulableProgramType programType,
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
          byte[] rowKey = Bytes.toBytes(getRowKey(programId, programType, scheduleName));
          table.put(rowKey, columns, values);
        }
      });
  }

  @Nullable
  String removeAppVersion(String scheduleId) {
    // New Row key for the trigger should be of the form -
    // streamSizeSchedule:namespace:application:version:type:program:schedule
    return ScheduleUpgradeUtil.splitAndRemoveDefaultVersion(scheduleId, 7, 3);
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

  /**
   * Method to add version in StreamSizeSchedule row key in SchedulerStore.
   *
   * @throws InterruptedException
   * @throws IOException
   * @throws DatasetManagementException
   */
  public void upgrade() throws InterruptedException, IOException, DatasetManagementException {
    // Wait until the store is initialized
    // Use a new instance of table since Table is not thread safe
    Table metaTable = null;
    while (metaTable == null) {
      try {
        metaTable = tableUtil.getMetaTable();
      } catch (Exception e) {
        // ignore exception
      }
      TimeUnit.SECONDS.sleep(10);
    }

    if (isUpgradeComplete()) {
      LOG.info("{} is already upgraded.", NAME);
      return;
    }

    final AtomicInteger maxNumberUpdateRows = new AtomicInteger(1000);
    final AtomicInteger sleepTimeInSecs = new AtomicInteger(60);
    LOG.info("Starting upgrade of {}.", NAME);
    while (!isUpgradeComplete()) {
      sleepTimeInSecs.set(60);
      try {
        final Table finalMetaTable = metaTable;
        factory.createExecutor(ImmutableList.of((TransactionAware) finalMetaTable))
          .execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() throws Exception {
              if (upgradeVersionKeys(finalMetaTable, maxNumberUpdateRows.get())) {
                // Upgrade is complete. Mark that app version upgrade is complete in the table.
                finalMetaTable.put(APP_VERSION_UPGRADE_KEY, COLUMN, Bytes.toBytes(ProjectInfo.getVersion().toString()));
              }
            }
          });
      } catch (TransactionFailureException e) {
        if (e instanceof TransactionConflictException) {
          LOG.debug("Upgrade step faced Transaction Conflict exception. Retrying operation now.", e);
          sleepTimeInSecs.set(10);
        } else if (e instanceof TransactionNotInProgressException) {
          int currMaxRows = maxNumberUpdateRows.get();
          if (currMaxRows > 500) {
            maxNumberUpdateRows.decrementAndGet();
          } else {
            LOG.warn("Could not complete upgrade of {}, tried for 500 times", NAME);
            return;
          }
          sleepTimeInSecs.set(10);
          LOG.debug("Upgrade step faced a Transaction Timeout exception. " +
                      "Current number of max update rows is set to : {} and retrying the operation now.",
                    maxNumberUpdateRows.get(), e);
        } else {
          LOG.error("Upgrade step faced exception. Will retry operation after some delay.", e);
          sleepTimeInSecs.set(60);
        }
      }
      TimeUnit.SECONDS.sleep(sleepTimeInSecs.get());
    }
    LOG.info("Upgrade of {} is complete.", NAME);
  }

  // Returns true if the upgrade flag is set. Upgrade could have completed earlier than this since this flag is
  // updated asynchronously.
  public boolean isUpgradeComplete() {
    if (upgradeCacheLoader == null) {
      return false;
    }
    return upgradeCacheLoader.getUnchecked(APP_VERSION_UPGRADE_KEY);
  }

  // Return whether the upgrade process is complete - determined by checking if there were no rows that were
  // upgraded after the invocation of this method.
  private boolean upgradeVersionKeys(Table table, int maxNumberUpdateRows) {
    int numRowsUpgraded = 0;
    try (Scanner scan = getScannerWithPrefix(table, KEY_PREFIX)) {
      Row next;
      // Upgrade only N rows in one transaction to reduce the probability of conflicts with regular Store operations.
      while (((next = scan.next()) != null) && (numRowsUpgraded < maxNumberUpdateRows)) {
        if (isInvalidRow(next)) {
          LIMITED_LOG.debug("Stream Sized Schedule entry with Row key {} does not have all columns.",
                            Bytes.toString(next.getRow()));
          continue;
        }
        byte[] oldRowKey = next.getRow();
        String oldRowKeyString = Bytes.toString(next.getRow());
        String[] splits = oldRowKeyString.split(":");
        // Row key for the trigger should be of the form -
        // streamSizeSchedule:namespace:application:type:program:schedule
        if (splits.length != 6) {
          LIMITED_LOG.debug("Skip upgrading StreamSizeSchedule {}. Expected row key " +
                     "format 'streamSizeSchedule:namespace:application:type:program:schedule'", oldRowKeyString);
          continue;
        }
        // append application version after application name
        byte[] newRowKey = Bytes.toBytes(ScheduleUpgradeUtil.getNameWithDefaultVersion(splits, 3));

        // Check if a newRowKey is already present, if it is present, then simply delete the oldRowKey and continue;
        Row row = table.get(newRowKey);
        if (!row.isEmpty()) {
          table.delete(oldRowKey);
          numRowsUpgraded++;
          continue;
        }

        Put put = new Put(newRowKey);
        for (Map.Entry<byte[], byte[]> colValEntry : next.getColumns().entrySet()) {
          put.add(colValEntry.getKey(), colValEntry.getValue());
        }
        table.put(put);
        table.delete(oldRowKey);
        numRowsUpgraded++;
      }
    }

    // If no rows were upgraded, notify that the upgrade process has completed.
    return (numRowsUpgraded == 0);
  }
}
