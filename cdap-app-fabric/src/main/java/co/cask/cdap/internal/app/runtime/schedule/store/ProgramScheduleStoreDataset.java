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
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.constraint.ConstraintCodec;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Dataset that stores and indexes program schedules, so that they can be looked by their trigger keys.
 *
 * This uses an IndexedTable to allow reverse lookup. The table stores:
 * <ul>
 *   <li>Schedules: the row key is
 *     <code>&lt;namespace>.&lt;app-name>.&lt;app-version>.&lt;schedule-name>)</code>,
 *     which is globally unique (see {@link #rowKeyForSchedule(ScheduleId)}. The schedule itself is stored as JSON
 *     in the <code>sch</code> ({@link #SCHEDULE_COLUMN} column.</li>
 *   <li>Triggers: as every schedule can have multiple triggers, each trigger is stored and indexed in its row. The
 *     triggers of a schedule are enumerated, and each trigger is stored with a row key that is the same as the
 *     schedule's row key, with <code>@&lt;sequential-id></code> appended. This ensures that a schedules and its
 *     triggered are stored in adjacent rows. The only column of trigger row is the trigger key (that is, the
 *     key that can be constructed from an event to look up the schedules that have a trigger for it), in column
 *     <code>tk</code> ({@link #TRIGGER_KEY_COLUMN}</li>.
 * </ul>
 *
 * Lookup of schedules by trigger key is by first finding the all triggers for that event key (using the index),
 * then mapping each of these triggers to the schedule it belongs to.
 */
public class ProgramScheduleStoreDataset extends AbstractDataset {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramScheduleStoreDataset.class);

  private static final String SCHEDULE_COLUMN = "sch";
  private static final byte[] SCHEDULE_COLUMN_BYTES = Bytes.toBytes(SCHEDULE_COLUMN);
  private static final String TRIGGER_KEY_COLUMN = "tk"; // trigger key
  private static final byte[] TRIGGER_KEY_COLUMN_BYTES = Bytes.toBytes(TRIGGER_KEY_COLUMN);

  // package visible for the dataset definition
  static final String EMBEDDED_TABLE_NAME = "it"; // indexed table
  static final String INDEX_COLUMNS = TRIGGER_KEY_COLUMN; // trigger key

  private static final Gson GSON =
    new GsonBuilder()
      .registerTypeAdapter(Constraint.class, new ConstraintCodec())
      .registerTypeAdapter(Trigger.class, new TriggerCodec())
      .create();

  private final IndexedTable store;

  ProgramScheduleStoreDataset(DatasetSpecification spec,
                              @EmbeddedDataset(EMBEDDED_TABLE_NAME) IndexedTable store) {
    super(spec.getName(), store);
    this.store = store;
  }

  /**
   * Add a schedule to the store.
   *
   * @param schedule the schedule to add
   * @throws AlreadyExistsException if the schedule already exists
   */
  public void addSchedule(ProgramSchedule schedule) throws AlreadyExistsException {
    addSchedules(Collections.singleton(schedule));
  }

  /**
   * Add one or more schedules to the store.
   *
   * @param schedules the schedules to add
   * @throws AlreadyExistsException if one of the schedules already exists
   */
  public void addSchedules(Iterable<? extends ProgramSchedule> schedules) throws AlreadyExistsException {
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
   * Add one or more schedules to the store.
   *
   * @param schedule the schedule to update
   * @throws NotFoundException if one of the schedules already exists
   */
  public void updateSchedule(ProgramSchedule schedule) throws NotFoundException {
    String scheduleKey = rowKeyForSchedule(schedule.getProgramId().getParent(), schedule.getName());
    if (store.get(new Get(scheduleKey)).isEmpty()) {
      throw new NotFoundException(schedule.getProgramId().getParent().schedule(schedule.getName()));
    }
    store.put(new Put(scheduleKey, SCHEDULE_COLUMN, GSON.toJson(schedule)));
    int count = 0;
    byte[] prefix = keyPrefixForTriggerScan(scheduleKey);
    try (Scanner scanner = store.scan(new Scan(prefix, Bytes.stopKeyForPrefix(prefix)))) {
      Row row;
      while ((row = scanner.next()) != null) {
        store.delete(row.getRow());
      }
    }
    for (String triggerKey : extractTriggerKeys(schedule)) {
      String triggerRowKey = rowKeyForTrigger(scheduleKey, count++);
      store.put(new Put(triggerRowKey, TRIGGER_KEY_COLUMN, triggerKey));
    }
  }

  /**
   * Removes a schedule from the store. Succeeds whether the schedule exists or not.
   *
   * @param scheduleId the schedule to delete
   * @throws NotFoundException if the schedule does not exist in the store
   */
  public void deleteSchedules(ScheduleId scheduleId) throws NotFoundException {
    deleteSchedules(Collections.singleton(scheduleId));
  }

  /**
   * Removes one or more schedules from the store. Succeeds whether the schedules exist or not.
   *
   * @param scheduleIds the schedules to delete
   * @throws NotFoundException if one of the schedules does not exist in the store
   */
  public void deleteSchedules(Iterable<? extends ScheduleId> scheduleIds) throws NotFoundException {
    for (ScheduleId scheduleId : scheduleIds) {
      String scheduleKey = rowKeyForSchedule(scheduleId);
      if (store.get(new Get(scheduleKey)).isEmpty()) {
        throw new NotFoundException(scheduleId);
      }
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
    // since all trigger row keys are prefixed by <scheduleRowKey>@,
    // a scan for that prefix finds exactly the schedules and all of its triggers
    byte[] prefix = keyPrefixForApplicationScan(appId);
    try (Scanner scanner = store.scan(new Scan(prefix, Bytes.stopKeyForPrefix(prefix)))) {
      Row row;
      while ((row = scanner.next()) != null) {
        store.delete(row.getRow());
      }
    }
  }

  /**
   * Removes all schedules for a specific program from the store.
   *
   * @param programId the program id for which to delete the schedules
   */
  public void deleteSchedules(ProgramId programId) {
    // since all trigger row keys are prefixed by <scheduleRowKey>@,
    // a scan for that prefix finds exactly the schedules and all of its triggers
    byte[] prefix = keyPrefixForApplicationScan(programId.getParent());
    try (Scanner scanner = store.scan(new Scan(prefix, Bytes.stopKeyForPrefix(prefix)))) {
      Row row;
      while ((row = scanner.next()) != null) {
        byte[] serialized = row.get(SCHEDULE_COLUMN_BYTES);
        if (serialized != null) {
          ProgramSchedule schedule = GSON.fromJson(Bytes.toString(serialized), ProgramSchedule.class);
          if (programId.equals(schedule.getProgramId())) {
            store.delete(row.getRow());
          }
        }
      }
    }
  }

  /**
   * Read a schedule from the store.
   *
   * @param scheduleId the id of the schedule to read
   * @return the schedule from the store
   * @throws NotFoundException if the schedule does not exist in the store
   */
  public ProgramSchedule getSchedule(ScheduleId scheduleId) throws NotFoundException {
    Row row = store.get(new Get(rowKeyForSchedule(scheduleId)));
    byte[] serialized = row.get(SCHEDULE_COLUMN_BYTES);
    if (serialized == null) {
      throw new NotFoundException(scheduleId);
    }
    return GSON.fromJson(Bytes.toString(serialized), ProgramSchedule.class);
  }

  /**
   * Retrieve all schedules for a given application.
   *
   * @param appId the application for which to list the schedules.
   * @return a list of schedules for the application; never null
   */
  public List<ProgramSchedule> listSchedules(ApplicationId appId) {
    return listSchedules(appId, null);
  }

  /**
   * Retrieve all schedules for a given program.
   *
   * @param programId the program for which to list the schedules.
   * @return a list of schedules for the program; never null
   */
  public List<ProgramSchedule> listSchedules(ProgramId programId) {
    return listSchedules(programId.getParent(), programId);
  }

  /**
   * Find all schedules that have a trigger with a given trigger key.
   *
   * @param triggerKey the trigger key to look up
   * @return a list of all schedules that are triggered by this key; never null
   */
  public Collection<ProgramSchedule> findSchedules(String triggerKey) {
    Map<ScheduleId, ProgramSchedule> schedulesFound = new HashMap<>();
    try (Scanner scanner = store.readByIndex(TRIGGER_KEY_COLUMN_BYTES, Bytes.toBytes(triggerKey))) {
      Row row;
      while ((row = scanner.next()) != null) {
        String triggerRowKey = Bytes.toString(row.getRow());
        try {
          ScheduleId scheduleId = extractScheduleIdFromTriggerKey(triggerRowKey);
          if (schedulesFound.containsKey(scheduleId)) {
            continue;
          }
          ProgramSchedule schedule = getSchedule(scheduleId);
          schedulesFound.put(scheduleId, schedule);
        } catch (IllegalArgumentException | NotFoundException e) {
          // the only exceptions we know to be thrown here are IllegalArgumentException (ill-formed key) or
          // NotFoundException (if the schedule does not exist). Both should never happen, so we warn and ignore.
          // we will let any other exception propagate up, because it would be a DataSetException or similarly serious.
          LOG.warn("Problem with trigger id '{}' found for trigger key '{}': {}. Skipping entry.",
                   triggerRowKey, triggerKey, e.getMessage());
        }
      }
    }
    return schedulesFound.values();
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

  /**
   * This method extracts all trigger keys from a schedule. These are the keys for which we need to index
   * the schedule, so that we can do a reverse lookup for an event received.
   *
   * For now, we do not support composite trigger, but in the future this is where the triggers need to be
   * extracted from composite triggers. Hence the return type of this method is a list.
   */
  private static List<String> extractTriggerKeys(ProgramSchedule schedule) {
    Trigger trigger = schedule.getTrigger();
    if (trigger instanceof PartitionTrigger) {
      String triggerKey = Schedulers.triggerKeyForPartition(((PartitionTrigger) trigger).getDataset());
      return Collections.singletonList(triggerKey);
    }
    return Collections.emptyList();
  }

  private static String rowKeyForSchedule(ApplicationId appId, String scheduleName) {
    return rowKeyForSchedule(appId.schedule(scheduleName));
  }

  private static String rowKeyForSchedule(ScheduleId scheduleId) {
    return Joiner.on('.').join(scheduleId.toIdParts());
  }

  private static ScheduleId rowKeyToScheduleId(String rowKey) {
    return ScheduleId.fromIdParts(Lists.newArrayList(rowKey.split("\\.")));
  }

  private static String rowKeyForTrigger(String scheduleRowKey, int count) {
    return scheduleRowKey + '@' + count;
  }

  private static ScheduleId extractScheduleIdFromTriggerKey(String triggerRowKey) {
    int index = triggerRowKey.lastIndexOf('@');
    if (index > 0) {
      return rowKeyToScheduleId(triggerRowKey.substring(0, index));
    }
    throw new IllegalArgumentException(
      "Trigger key is expected to be of the form <scheduleId>@<n> but is '" + triggerRowKey + "' (no '@' found)");
  }

  private static byte[] keyPrefixForTriggerScan(String scheduleRowKey) {
    return Bytes.toBytes(scheduleRowKey + '@');
  }

  private static byte[] keyPrefixForApplicationScan(ApplicationId appId) {
    return Bytes.toBytes(appId.getNamespace() + '.' + appId.getApplication() + '.' + appId.getVersion());
  }
}
