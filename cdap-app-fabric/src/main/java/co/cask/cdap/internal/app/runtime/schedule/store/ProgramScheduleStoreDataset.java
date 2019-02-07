/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleMeta;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.constraint.ConstraintCodec;
import co.cask.cdap.internal.app.runtime.schedule.trigger.AbstractSatisfiableCompositeTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.spi.data.InvalidFieldException;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.Fields;
import co.cask.cdap.spi.data.table.field.Range;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

// TODO: (poorna) fix javadoc
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
public class ProgramScheduleStoreDataset {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramScheduleStoreDataset.class);

  private static final Gson GSON =
    new GsonBuilder()
      .registerTypeAdapter(Constraint.class, new ConstraintCodec())
      .registerTypeAdapter(Trigger.class, new TriggerCodec())
      .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
      .create();

  private final StructuredTable scheduleStore;
  private final StructuredTable triggerStore;

  ProgramScheduleStoreDataset(StructuredTable scheduleStore, StructuredTable triggerStore) {
    this.scheduleStore = scheduleStore;
    this.triggerStore = triggerStore;
  }

  /**
   * Add a schedule to the store.
   *
   * @param schedule the schedule to add
   * @return the new schedule's last modified timestamp
   * @throws AlreadyExistsException if the schedule already exists
   */
  public long addSchedule(ProgramSchedule schedule) throws AlreadyExistsException, IOException {
    return addSchedules(Collections.singleton(schedule));
  }

  /**
   * Add a schedule to the store.
   *
   * @param schedule the schedule to add
   * @param status the status of the schedule to add
   * @param currentTime the current time in milliseconds when adding the schedule
   * @throws AlreadyExistsException if the schedule already exists
   */
  private void addScheduleWithStatus(ProgramSchedule schedule, ProgramScheduleStatus status, long currentTime)
    throws AlreadyExistsException, IOException {
    Collection<Field<?>> scheduleKeys =
      getScheduleKeys(schedule.getProgramId().getParent().schedule(schedule.getName()));
    if (scheduleStore.read(scheduleKeys).isPresent()) {
      throw new AlreadyExistsException(schedule.getProgramId().getParent().schedule(schedule.getName()));
    }
    
    Collection<Field<?>> scheduleFields = new ArrayList<>(scheduleKeys);
    scheduleFields.add(Fields.stringField(StoreDefinition.ProgramScheduleStore.SCHEDULE, GSON.toJson(schedule)));
    scheduleFields.add(Fields.longField(StoreDefinition.ProgramScheduleStore.UPDATE_TIME, currentTime));
    scheduleFields.add(Fields.stringField(StoreDefinition.ProgramScheduleStore.STATUS, status.toString()));
    scheduleStore.upsert(scheduleFields);

    int count = 0;
    for (String triggerKey : extractTriggerKeys(schedule)) {
      Collection<Field<?>> triggerFields = getTriggerKeys(scheduleKeys, count++);
      triggerFields.add(Fields.stringField(StoreDefinition.ProgramScheduleStore.TRIGGER_KEY, triggerKey));
      triggerStore.upsert(triggerFields);
    }
  }

  /**
   * Add one or more schedules to the store.
   *
   * @param schedules the schedules to add
   * @return the new schedules' last modified timestamp
   * @throws AlreadyExistsException if one of the schedules already exists
   */
  public long addSchedules(Iterable<? extends ProgramSchedule> schedules) throws AlreadyExistsException, IOException {
    long currentTime = System.currentTimeMillis();
    for (ProgramSchedule schedule : schedules) {
      addScheduleWithStatus(schedule, ProgramScheduleStatus.SUSPENDED, currentTime); // initially suspended
    }
    return currentTime;
  }

  /**
   * Update the status of a schedule. This also updates the last-updated timestamp.
   * @return the updated schedule's last modified timestamp
   */
  public long updateScheduleStatus(ScheduleId scheduleId, ProgramScheduleStatus newStatus)
    throws NotFoundException, IOException {
    long currentTime = System.currentTimeMillis();
    Collection<Field<?>> scheduleFields = getScheduleKeys(scheduleId);
    if (!scheduleStore.read(scheduleFields).isPresent()) {
      throw new NotFoundException(scheduleId);
    }

    // record current time
    scheduleFields.add(Fields.longField(StoreDefinition.ProgramScheduleStore.UPDATE_TIME, currentTime));
    scheduleFields.add(Fields.stringField(StoreDefinition.ProgramScheduleStore.STATUS, newStatus.toString()));
    scheduleStore.upsert(scheduleFields);
    return currentTime;
  }

  /**
   * Update an existing schedule in the store.
   *
   * @param schedule the schedule to update
   * @return the updated schedule's last modified timestamp
   * @throws NotFoundException if one of the schedules already exists
   */
  public long updateSchedule(ProgramSchedule schedule) throws NotFoundException, IOException {
    deleteSchedule(schedule.getScheduleId());
    try {
      return addSchedule(schedule);
    } catch (AlreadyExistsException e) {
      // Should never reach here because we just deleted it
      throw new IllegalStateException(
        "Schedule '" + schedule.getScheduleId() + "' already exists despite just being deleted.");
    }
  }

  /**
   * Removes a schedule from the store. Succeeds whether the schedule exists or not.
   *
   * @param scheduleId the schedule to delete
   * @throws NotFoundException if the schedule does not exist in the store
   */
  public void deleteSchedule(ScheduleId scheduleId) throws NotFoundException, IOException {
    deleteSchedules(Collections.singleton(scheduleId));
  }

  /**
   * Removes one or more schedules from the store. Succeeds whether the schedules exist or not.
   *
   * @param scheduleIds the schedules to delete
   * @throws NotFoundException if one of the schedules does not exist in the store
   */
  public void deleteSchedules(Iterable<? extends ScheduleId> scheduleIds) throws NotFoundException, IOException {
    for (ScheduleId scheduleId : scheduleIds) {
      Collection<Field<?>> scheduleKeys = getScheduleKeys(scheduleId);
      if (!scheduleStore.read(scheduleKeys).isPresent()) {
        throw new NotFoundException(scheduleId);
      }
      scheduleStore.delete(scheduleKeys);
      triggerStore.deleteAll(Range.singleton(scheduleKeys));
    }
  }

  /**
   * Removes all schedules for a specific application from the store.
   *
   * @param appId the application id for which to delete the schedules
   * @return the IDs of the schedules that were deleted
   */
  // TODO: fix the bug that this method will return fake schedule id https://issues.cask.co/browse/CDAP-13626
  public List<ScheduleId> deleteSchedules(ApplicationId appId) throws IOException {
    List<ScheduleId> deleted = new ArrayList<>();
    Collection<Field<?>> scanKeys = getScheduleKeysForApplicationScan(appId);
    Range range = Range.singleton(scanKeys);
    // First collect all the schedules that are going to be deleted
    try (CloseableIterator<StructuredRow> iterator = scheduleStore.scan(range, Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        deleted.add(rowToScheduleId(iterator.next()));
      }
    }
    // Then delete both schedules and the triggers for the app
    scheduleStore.deleteAll(range);
    triggerStore.deleteAll(range);
    return deleted;
  }

  /**
   * Removes all schedules for a specific program from the store.
   *
   * @param programId the program id for which to delete the schedules
   * @return the IDs of the schedules that were deleted
   */
  // TODO: fix the bug that this method will return fake schedule id https://issues.cask.co/browse/CDAP-13626
  public List<ScheduleId> deleteSchedules(ProgramId programId) throws IOException {
    List<ScheduleId> deleted = new ArrayList<>();
    Collection<Field<?>> scanKeys = getScheduleKeysForApplicationScan(programId.getParent());
    Range range = Range.singleton(scanKeys);
    // First collect all the schedules that are going to be deleted
    try (CloseableIterator<StructuredRow> iterator = scheduleStore.scan(range, Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        String serializedSchedule = row.getString(StoreDefinition.ProgramScheduleStore.SCHEDULE);
        if (serializedSchedule != null) {
          ProgramSchedule schedule = GSON.fromJson(serializedSchedule, ProgramSchedule.class);
          if (programId.equals(schedule.getProgramId())) {
            deleted.add(rowToScheduleId(row));
            Collection<Field<?>> deleteKeys = getScheduleKeys(row);
            scheduleStore.delete(deleteKeys);
            triggerStore.deleteAll(Range.singleton(deleteKeys));
          }
        }
      }
    }
    return deleted;
  }

  /**
   * Update all schedules that can be triggered by the given deleted program. A schedule will be removed if
   * the only {@link ProgramStatusTrigger} in it is triggered by the deleted program. Schedules with composite triggers
   * will be updated if the composite trigger can still be satisfied after the program is deleted, otherwise the
   * schedules will be deleted.
   *
   * @param programId the program id for which to delete the schedules
   * @return the IDs of the schedules that were deleted
   */
  public List<ScheduleId> modifySchedulesTriggeredByDeletedProgram(ProgramId programId) throws IOException {
    List<ScheduleId> deleted = new ArrayList<>();
    Set<ProgramScheduleRecord> scheduleRecords = new HashSet<>();
    for (ProgramStatus status : ProgramStatus.values()) {
      scheduleRecords.addAll(findSchedules(Schedulers.triggerKeyForProgramStatus(programId, status)));
    }
    for (ProgramScheduleRecord scheduleRecord : scheduleRecords) {
      ProgramSchedule schedule = scheduleRecord.getSchedule();
      try {
        deleteSchedule(schedule.getScheduleId());
      } catch (NotFoundException e) {
        // this should never happen
        LOG.warn("Failed to delete the schedule '{}' triggered by '{}', skip this schedule.",
                 schedule.getScheduleId(), programId, e);
        continue;
      }
      if (schedule.getTrigger() instanceof AbstractSatisfiableCompositeTrigger) {
        // get the updated composite trigger by removing the program status trigger of the given program
        Trigger updatedTrigger =
          ((AbstractSatisfiableCompositeTrigger) schedule.getTrigger()).getTriggerWithDeletedProgram(programId);
        if (updatedTrigger == null) {
          deleted.add(schedule.getScheduleId());
          continue;
        }
        // if the updated composite trigger is not null, add the schedule back with updated composite trigger
        try {
          addScheduleWithStatus(new ProgramSchedule(schedule.getName(), schedule.getDescription(),
                                                    schedule.getProgramId(), schedule.getProperties(), updatedTrigger,
                                                    schedule.getConstraints(), schedule.getTimeoutMillis()),
                                scheduleRecord.getMeta().getStatus(), System.currentTimeMillis());
        } catch (AlreadyExistsException e) {
          // this should never happen
          LOG.warn("Failed to add the schedule '{}' triggered by '{}' with updated trigger '{}', " +
                     "skip adding this schedule.", schedule.getScheduleId(), programId, updatedTrigger, e);
        }
      } else {
        deleted.add(schedule.getScheduleId());
      }
    }
    return deleted;
  }

  /**
   * Read a schedule from the store.
   *
   * @param scheduleId the id of the schedule to read
   * @return the schedule from the store
   * @throws NotFoundException if the schedule does not exist in the store
   */
  public ProgramSchedule getSchedule(ScheduleId scheduleId) throws NotFoundException, IOException {
    StructuredRow row = readScheduleRow(scheduleId);
    String serializedSchedule = row.getString(StoreDefinition.ProgramScheduleStore.SCHEDULE);
    if (serializedSchedule == null) {
      throw new NotFoundException(scheduleId);
    }
    return GSON.fromJson(serializedSchedule, ProgramSchedule.class);
  }

  /**
   * Read all information about a schedule from the store.
   *
   * @param scheduleId the id of the schedule to read
   * @return the schedule record from the store
   * @throws NotFoundException if the schedule does not exist in the store
   */
  public ProgramScheduleRecord getScheduleRecord(ScheduleId scheduleId) throws NotFoundException, IOException {
    StructuredRow row = readScheduleRow(scheduleId);
    String serializedSchedule = row.getString(StoreDefinition.ProgramScheduleStore.SCHEDULE);
    if (serializedSchedule == null) {
      throw new NotFoundException(scheduleId);
    }
    ProgramSchedule schedule = GSON.fromJson(serializedSchedule, ProgramSchedule.class);
    ProgramScheduleMeta meta = extractMetaFromRow(scheduleId, row);
    return new ProgramScheduleRecord(schedule, meta);
  }

  /**
   * Retrieve all schedules for a given namespace.
   *
   * @param namespaceId the namespace for which to list the schedules
   * @param filter the filter to be applied on the result schedules
   * @return a list of schedules for the namespace; never null
   */
  public List<ProgramSchedule> listSchedules(NamespaceId namespaceId, Predicate<ProgramSchedule> filter)
    throws IOException {
    return listSchedulesWithPrefix(getScheduleKeysForNamespaceScan(namespaceId), filter);
  }

  /**
   * Retrieve all schedules for a given application.
   *
   * @param appId the application for which to list the schedules.
   * @return a list of schedules for the application; never null
   */
  public List<ProgramSchedule> listSchedules(ApplicationId appId) throws IOException {
    return listSchedulesWithPrefix(getScheduleKeysForApplicationScan(appId), schedule -> true);
  }

  /**
   * Retrieve all schedules for a given program.
   *
   * @param programId the program for which to list the schedules.
   * @return a list of schedules for the program; never null
   */
  public List<ProgramSchedule> listSchedules(ProgramId programId) throws IOException {
    return listSchedulesWithPrefix(getScheduleKeysForApplicationScan(programId.getParent()),
                                   schedule -> programId.equals(schedule.getProgramId()));
  }

  /**
   * Retrieve all schedule records for a given application.
   *
   * @param appId the application for which to list the schedule records.
   * @return a list of schedule records for the application; never null
   */
  public List<ProgramScheduleRecord> listScheduleRecords(ApplicationId appId) throws IOException {
    return listSchedulesRecordsWithPrefix(getScheduleKeysForApplicationScan(appId), schedule -> true);
  }

  /**
   * Retrieve all schedule records for a given program.
   *
   * @param programId the program for which to list the schedule records.
   * @return a list of schedule records for the program; never null
   */
  public List<ProgramScheduleRecord> listScheduleRecords(ProgramId programId) throws IOException {
    return listSchedulesRecordsWithPrefix(getScheduleKeysForApplicationScan(programId.getParent()),
                                          schedule -> programId.equals(schedule.getProgramId()));
  }

  /**
   * Find all schedules that have a trigger with a given trigger key.
   *
   * @param triggerKey the trigger key to look up
   * @return a list of all schedules that are triggered by this key; never null
   */
  public Collection<ProgramScheduleRecord> findSchedules(String triggerKey) throws IOException {
    Map<ScheduleId, ProgramScheduleRecord> schedulesFound = new HashMap<>();
    Field<String> triggerField = Fields.stringField(StoreDefinition.ProgramScheduleStore.TRIGGER_KEY, triggerKey);
    try (CloseableIterator<StructuredRow> iterator = triggerStore.scan(triggerField)) {
      while (iterator.hasNext()) {
        StructuredRow triggerRow = iterator.next();
        try {
          ScheduleId scheduleId = rowToScheduleId(triggerRow);
          if (schedulesFound.containsKey(scheduleId)) {
            continue;
          }
          Optional<StructuredRow> optional = scheduleStore.read(getScheduleKeys(scheduleId));
          if (!optional.isPresent()) {
            throw new NotFoundException(scheduleId);
          }
          StructuredRow scheduleRow = optional.get();
          String serialized = scheduleRow.getString(StoreDefinition.ProgramScheduleStore.SCHEDULE);
          if (serialized == null) {
            throw new NotFoundException(scheduleId);
          }
          ProgramSchedule schedule = GSON.fromJson(serialized, ProgramSchedule.class);
          ProgramScheduleMeta meta = extractMetaFromRow(scheduleId, scheduleRow);
          ProgramScheduleRecord record = new ProgramScheduleRecord(schedule, meta);
          schedulesFound.put(scheduleId, record);
        } catch (IllegalArgumentException | NotFoundException e) {
          // the only exceptions we know to be thrown here are IllegalArgumentException (ill-formed key) or
          // NotFoundException (if the schedule does not exist). Both should never happen, so we warn and ignore.
          // we will let any other exception propagate up, because it would be a DataSetException or similarly serious.
          LOG.warn("Problem with trigger '{}' found for trigger key '{}': {}. Skipping entry.",
                   triggerRow, triggerKey, e.getMessage());
        }
      }
    }
    return schedulesFound.values();
  }

  /*------------------- private helpers ---------------------*/

  /**
   * List schedules with the given key prefix and only returns the schedules that can pass the filter.
   *
   * @param prefixKeys the prefix of the schedule records to be listed
   * @param filter a filter that only returns true if the schedule will be returned in the result
   * @return the schedules with the given key prefix that can pass the filter
   */
  private List<ProgramSchedule> listSchedulesWithPrefix(Collection<Field<?>> prefixKeys,
                                                        Predicate<ProgramSchedule> filter) throws IOException {
    List<ProgramSchedule> result = new ArrayList<>();
    try (CloseableIterator<StructuredRow> iterator =
           scheduleStore.scan(Range.singleton(prefixKeys), Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        String serializedSchedule = row.getString(StoreDefinition.ProgramScheduleStore.SCHEDULE);
        if (serializedSchedule != null) {
          ProgramSchedule schedule = GSON.fromJson(serializedSchedule, ProgramSchedule.class);
          if (schedule != null && filter.test(schedule)) {
            result.add(schedule);
          }
        }
      }
    }
    return result;
  }

  /**
   * List schedule records with the given key prefix and only returns the schedules that can pass the filter.
   *
   * @param prefixKeys the prefix of the schedule records to be listed
   * @param filter a filter that only returns true if the schedule record will be returned in the result
   * @return the schedule records with the given key prefix that can pass the filter
   */
  private List<ProgramScheduleRecord> listSchedulesRecordsWithPrefix(Collection<Field<?>> prefixKeys,
                                                                     Predicate<ProgramSchedule> filter)
    throws IOException {
    List<ProgramScheduleRecord> result = new ArrayList<>();
    try (CloseableIterator<StructuredRow> iterator =
           scheduleStore.scan(Range.singleton(prefixKeys), Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        String serializedSchedule = row.getString(StoreDefinition.ProgramScheduleStore.SCHEDULE);
        if (serializedSchedule != null) {
          ProgramSchedule schedule = GSON.fromJson(serializedSchedule, ProgramSchedule.class);
          if (schedule != null && filter.test(schedule)) {
            result.add(new ProgramScheduleRecord(schedule, extractMetaFromRow(schedule.getScheduleId(), row)));
          }
        }
      }
    }
    return result;
  }

  private StructuredRow readScheduleRow(ScheduleId scheduleId) throws IOException, NotFoundException {
    Collection<Field<?>> scheduleKeys = getScheduleKeys(scheduleId);
    Optional<StructuredRow> rowOptional = scheduleStore.read(scheduleKeys);
    if (!rowOptional.isPresent()) {
      throw new NotFoundException(scheduleId);
    }
    return rowOptional.get();
  }

  /**
   * Reads the meta data from a row in the schedule store.
   *
   * @throws IllegalStateException if one of the expected fields is missing or ill-formed.
   */
  private ProgramScheduleMeta extractMetaFromRow(ScheduleId scheduleId, StructuredRow row) {
    Long updatedTime = row.getLong(StoreDefinition.ProgramScheduleStore.UPDATE_TIME);
    String statusString = row.getString(StoreDefinition.ProgramScheduleStore.STATUS);
    try {
      Preconditions.checkArgument(updatedTime != null, "Last-updated timestamp is null");
      Preconditions.checkArgument(statusString != null, "schedule status is null");
      ProgramScheduleStatus status = ProgramScheduleStatus.valueOf(statusString);
      return new ProgramScheduleMeta(status, updatedTime);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
        String.format("Unexpected stored meta data for schedule %s: %s", scheduleId, e.getMessage()));
    }
  }

  /**
   * This method extracts all trigger keys from a schedule. These are the keys for which we need to index
   * the schedule, so that we can do a reverse lookup for an event received.
   * <p>
   * For now, we do not support composite trigger, but in the future this is where the triggers need to be
   * extracted from composite triggers. Hence the return type of this method is a list.
   */
  private static Set<String> extractTriggerKeys(ProgramSchedule schedule) {
    return ((SatisfiableTrigger) schedule.getTrigger()).getTriggerKeys();
  }

  private static Collection<Field<?>> getScheduleKeys(ScheduleId scheduleId) {
    List<Field<?>> keys = new ArrayList<>();
    keys.add(Fields.stringField(StoreDefinition.ProgramScheduleStore.NAMESPACE_FIELD, scheduleId.getNamespace()));
    keys.add(Fields.stringField(StoreDefinition.ProgramScheduleStore.APPLICATION_FIELD, scheduleId.getApplication()));
    keys.add(Fields.stringField(StoreDefinition.ProgramScheduleStore.VERSION_FIELD, scheduleId.getVersion()));
    keys.add(Fields.stringField(StoreDefinition.ProgramScheduleStore.SCHEDULE_NAME, scheduleId.getSchedule()));
    return keys;
  }

  private static Collection<Field<?>> getScheduleKeys(StructuredRow row) {
    List<Field<?>> keys = new ArrayList<>();
    keys.add(getStringKeyField(row, StoreDefinition.ProgramScheduleStore.NAMESPACE_FIELD));
    keys.add(getStringKeyField(row, StoreDefinition.ProgramScheduleStore.APPLICATION_FIELD));
    keys.add(getStringKeyField(row, StoreDefinition.ProgramScheduleStore.VERSION_FIELD));
    keys.add(getStringKeyField(row, StoreDefinition.ProgramScheduleStore.SCHEDULE_NAME));
    return keys;
  }

  private static Field<?> getStringKeyField(StructuredRow row, String name) {
    String value = row.getString(name);
    if (value == null) {
      throw new InvalidFieldException(StoreDefinition.ProgramScheduleStore.PROGRAM_SCHEDULE_STORE_TABLE, name,
                                      " is null. It should not be null as it is part of the primary key");
    }
    return Fields.stringField(name, value);
  }

  private static ScheduleId rowToScheduleId(StructuredRow row) {
    return new ScheduleId(row.getString(StoreDefinition.ProgramScheduleStore.NAMESPACE_FIELD),
                          row.getString(StoreDefinition.ProgramScheduleStore.APPLICATION_FIELD),
                          row.getString(StoreDefinition.ProgramScheduleStore.VERSION_FIELD),
                          row.getString(StoreDefinition.ProgramScheduleStore.SCHEDULE_NAME));
  }

  private static Collection<Field<?>> getTriggerKeys(Collection<Field<?>> scheduleKeys, int count) {
    List<Field<?>> keys = new ArrayList<>(scheduleKeys);
    keys.add(Fields.intField(StoreDefinition.ProgramScheduleStore.SEQUENCE_ID, count));
    return keys;
  }

  private static Collection<Field<?>> getScheduleKeysForApplicationScan(ApplicationId appId) {
    List<Field<?>> keys = new ArrayList<>();
    keys.add(Fields.stringField(StoreDefinition.ProgramScheduleStore.NAMESPACE_FIELD, appId.getNamespace()));
    keys.add(Fields.stringField(StoreDefinition.ProgramScheduleStore.APPLICATION_FIELD, appId.getApplication()));
    keys.add(Fields.stringField(StoreDefinition.ProgramScheduleStore.VERSION_FIELD, appId.getVersion()));
    return keys;
  }

  private static Collection<Field<?>> getScheduleKeysForNamespaceScan(NamespaceId namespaceId) {
    Field<String> field =
      Fields.stringField(StoreDefinition.ProgramScheduleStore.NAMESPACE_FIELD, namespaceId.getNamespace());
    return Collections.singleton(field);
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework}. Used by the upgrade tool to upgrade Datasets
   *
   * @param datasetFramework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework datasetFramework) throws IOException,
    DatasetManagementException {
    datasetFramework.addInstance(Schedulers.STORE_TYPE_NAME, Schedulers.STORE_DATASET_ID, DatasetProperties.EMPTY);
  }
}
