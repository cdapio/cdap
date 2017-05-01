/*
 * Copyright © 2017 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.Trigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TriggerJsonDeserializer;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Dataset that stores and oindexes proram schedules, so that they can be looked by their trigger keys.
 */
public class ProgramScheduleStoreDataset extends AbstractDataset {

  private static final String SCHEDULE_COLUMN = "sch";
  private static final byte[] SCHEDULE_COLUMN_BYTES = Bytes.toBytes(SCHEDULE_COLUMN);
  private static final String TRIGGER_KEY_COLUMN = "tk"; // trigger key
  private static final byte[] TRIGGER_KEY_COLUMN_BYTES = Bytes.toBytes(TRIGGER_KEY_COLUMN);

  // package visible for the dataset definition
  static final String EMBEDDED_TABLE_NAME = "it"; // indexed table
  static final String INDEX_COLUMNS = TRIGGER_KEY_COLUMN; // trigger key

  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Trigger.class, new TriggerJsonDeserializer()).create();

  private final IndexedTable store;

  public ProgramScheduleStoreDataset(DatasetSpecification spec,
                                     @EmbeddedDataset(EMBEDDED_TABLE_NAME) IndexedTable store) {
    super(spec.getName(), store);
    this.store = store;
  }

  /**
   * Add one or more schedules to the store.
   *
   * @param schedules the schedules to add
   *
   * @throws AlreadyExistsException if one of the schedules already exists
   */
  public void addSchedules(ProgramSchedule... schedules) throws AlreadyExistsException {
    for (ProgramSchedule schedule : schedules) {
      String scheduleKey = rowKeyForSchedule(schedule.getProgramId().getParent(), schedule.getName());
      if (!store.get(new Get(scheduleKey)).isEmpty()) {
        throw new AlreadyExistsException(schedule.getProgramId().getParent().schedule(schedule.getName()));
      }
      store.put(new Put(scheduleKey, SCHEDULE_COLUMN, GSON.toJson(schedule)));
      int count = 0;
      for (String triggerKey : extractTriggerKeys(schedule)) {
        String triggerRowKey = rowKeyForTrigger(scheduleKey, count++);
        store.put(new Put(triggerRowKey, TRIGGER_KEY_COLUMN, triggerKey));
      }
    }
  }

  /**
   * Removes one or more schedules from the store. Succeeds whether the schedules exist or not.
   *
   * @param scheduleIds the schedules to delete
   */
  public void deleteSchedules(ScheduleId ... scheduleIds) {
    for (ScheduleId scheduleId : scheduleIds) {
      String scheduleKey = rowKeyForSchedule(scheduleId);
      store.delete(new Delete(scheduleKey));
      byte[] prefix = keyPrefixForTriggerScan(scheduleKey);
      try (Scanner scanner = store.scan(new Scan(prefix, Bytes.stopKeyForPrefix(prefix)))) {
        Row row;
        while ((row = scanner.next()) != null) {
          store.delete(row.getRow());
        }
      }
    }
  }

  /**
   * Removes all schedules for a specific application from the store.
   *
   * @param appId the application id for which to delete the schedules
   */
  public void deleteSchedules(ApplicationId appId) {
    byte[] prefix = keyPrefixForApplicationScan(appId);
    try (Scanner scanner = store.scan(new Scan(prefix, Bytes.stopKeyForPrefix(prefix)))) {
      Row row;
      while ((row = scanner.next()) != null) {
        store.delete(row.getRow());
      }
    }
  }

  /**
   * Read a schedule from the store.
   *
   * @param scheduleId the id of the schedule to read
   *
   * @return the schedule from the store, or null if it was not found
   */
  @Nullable
  public ProgramSchedule getSchedule(ScheduleId scheduleId) {
    Row row = store.get(new Get(rowKeyForSchedule(scheduleId)));
    byte[] serialized = row.get(SCHEDULE_COLUMN_BYTES);
    return serialized == null ? null : GSON.fromJson(Bytes.toString(serialized), ProgramSchedule.class);
  }

  /**
   * Retrieve all schedules for a given application.
   *
   * @param appId the application for which to list the schedules.
   *
   * @return a list of schedules for the application; never null
   */
  public List<ProgramSchedule> listSchedules(ApplicationId appId) {
    return listSchedules(appId, null);
  }

  /**
   * Retrieve all schedules for a given program.
   *
   * @param programId the program for which to list the schedules.
   *
   * @return a list of schedules for the program; never null
   */
  public List<ProgramSchedule> listSchedules(ProgramId programId) {
    return listSchedules(programId.getParent(), programId);
  }

  /**
   * Find all schedules that have a trigger with a given trigger key.
   *
   * @param triggerKey the trigger key to look up
   *
   * @return a list of all schedules that are triggered by this key; never null
   */
  public List<ProgramSchedule> findSchedules(String triggerKey) {
    List<ProgramSchedule> result = new ArrayList<>();
    try (Scanner scanner = store.readByIndex(TRIGGER_KEY_COLUMN_BYTES, Bytes.toBytes(triggerKey))) {
      Row row;
      while ((row = scanner.next()) != null) {
        byte[] serialized = row.get(SCHEDULE_COLUMN_BYTES);
        if (serialized != null) {
          ProgramSchedule schedule = GSON.fromJson(Bytes.toString(serialized), ProgramSchedule.class);
          result.add(schedule);
        }
      }
    }
    return result;
  }

  /*------------------- private helpers ---------------------*/

  private List<ProgramSchedule> listSchedules(ApplicationId appId, @Nullable ProgramId programId) {
    List<ProgramSchedule> result = new ArrayList<>();
    byte[] prefix = keyPrefixForApplicationScan(appId);
    try (Scanner scanner = store.scan(new Scan(prefix, Bytes.stopKeyForPrefix(prefix)))) {
      Row row;
      while ((row = scanner.next()) != null) {
        byte[] serialized = row.get(SCHEDULE_COLUMN_BYTES);
        if (serialized != null) {
          ProgramSchedule schedule = GSON.fromJson(Bytes.toString(serialized), ProgramSchedule.class);
          if (programId == null || programId.equals(schedule.getProgramId())) {
            result.add(schedule);
          }
        }
      }
    }
    return result;
  }

  private List<String> extractTriggerKeys(ProgramSchedule schedule) {
    Trigger trigger = schedule.getTrigger();
    if (trigger instanceof PartitionTrigger) {
      String triggerKey = TriggerKeys.triggerKeyForPartition(((PartitionTrigger) trigger).getDatasetId());
      return Collections.singletonList(triggerKey);
    }
    return Collections.emptyList();
  }

  private String rowKeyForSchedule(ApplicationId appId, String scheduleName) {
    return appId.getNamespace() + '.' + appId.getApplication() + '.' + scheduleName;
  }

  private String rowKeyForSchedule(ScheduleId scheduleId) {
    return rowKeyForSchedule(scheduleId.getParent(), scheduleId.getSchedule());
  }

  private String rowKeyForTrigger(String scheduleRowKey, int count) {
    return scheduleRowKey + '@' + count;
  }

  private byte[] keyPrefixForTriggerScan(String scheduleRowKey) {
    return Bytes.toBytes(scheduleRowKey + '@');
  }

  private byte[] keyPrefixForApplicationScan(ApplicationId appId) {
    return Bytes.toBytes(appId.getNamespace() + '.' + appId.getApplication() + '.');
  }
}
