/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.app.store.ScanSourceControlMetadataRequest;
import io.cdap.cdap.proto.SourceControlMetadataRecord;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.sourcecontrol.SortBy;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Store for namespace source control metadata.
 */
public class NamespaceSourceControlMetadataStore {

  private StructuredTable namespaceSourceControlMetadataTable;
  private final StructuredTableContext context;

  public static NamespaceSourceControlMetadataStore create(StructuredTableContext context) {
    return new NamespaceSourceControlMetadataStore(context);
  }

  private NamespaceSourceControlMetadataStore(StructuredTableContext context) {
    this.context = context;
  }

  private StructuredTable getNamespaceSourceControlMetadataTable() {
    try {
      if (namespaceSourceControlMetadataTable == null) {
        namespaceSourceControlMetadataTable = context.getTable(
            StoreDefinition.NamespaceSourceControlMetadataStore.NAMESPACE_SOURCE_CONTROL_METADATA);
      }
    } catch (TableNotFoundException e) {
      throw new TableNotFoundException(
          StoreDefinition.NamespaceSourceControlMetadataStore.NAMESPACE_SOURCE_CONTROL_METADATA);
    }
    return namespaceSourceControlMetadataTable;
  }

  /**
   * Retrieves the source control metadata for the specified application ID in the namespace from
   * {@code NamespaceSourceControlMetadata} table.
   *
   * @param appRef {@link ApplicationReference} for which the source control metadata is being
   *               retrieved.
   * @return The {@link SourceControlMeta} associated with the application ID, or {@code null} if no
   *         metadata is found.
   * @throws IOException If it fails to read the metadata.
   */
  @Nullable
  public SourceControlMeta get(ApplicationReference appRef) throws IOException {
    List<Field<?>> primaryKey = getPrimaryKey(appRef);
    StructuredTable table = getNamespaceSourceControlMetadataTable();
    Optional<StructuredRow> row = table.read(primaryKey);

    return row.map(nonNullRow -> {
      String specificationHash = nonNullRow.getString(
          StoreDefinition.NamespaceSourceControlMetadataStore.SPECIFICATION_HASH_FIELD);
      String commitId = nonNullRow.getString(
          StoreDefinition.NamespaceSourceControlMetadataStore.COMMIT_ID_FIELD);
      Long lastSynced = nonNullRow.getLong(
          StoreDefinition.NamespaceSourceControlMetadataStore.LAST_MODIFIED_FIELD);
      Instant lastSyncedInstant = lastSynced == null || lastSynced == 0L ? null
          : Instant.ofEpochMilli(lastSynced);
      Boolean isSynced = nonNullRow.getBoolean(
          StoreDefinition.NamespaceSourceControlMetadataStore.IS_SYNCED_FIELD);
      return new SourceControlMeta(specificationHash, commitId, lastSyncedInstant,
          isSynced != null && isSynced);
    }).orElse(null);
  }

  /**
   * Scans the source control metadata records based on the specified request parameters, and
   * invokes the provided consumer for each matched record.
   *
   * @param request  The {@link ScanSourceControlMetadataRequest} object specifying the criteria for
   *                 scanning the metadata records.
   * @param type     The type of metadata records to scan. Currently, it is
   *                 {@code APPLICATIONREFERENCE} by default.
   * @param consumer The consumer to be invoked for each matched metadata record.
   * @return The number of metadata records scanned and processed.
   * @throws IOException If an I/O error occurs while scanning the metadata records.
   */
  public int scan(ScanSourceControlMetadataRequest request,
      String type, Consumer<SourceControlMetadataRecord> consumer) throws IOException {
    Range.Bound startBound = Range.Bound.INCLUSIVE;
    final Range.Bound endBound = Range.Bound.INCLUSIVE;
    Collection<Field<?>> startFields = new ArrayList<>();
    StructuredTable table = getNamespaceSourceControlMetadataTable();

    startFields.add(
        Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.NAMESPACE_FIELD,
            request.getNamespace())
    );
    startFields.add(
        Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.TYPE_FIELD, type)
    );

    if (request.getFilter().getIsSynced() != null) {
      startFields.add(
          Fields.booleanField(StoreDefinition.NamespaceSourceControlMetadataStore.IS_SYNCED_FIELD,
              request.getFilter().getIsSynced()));
    }

    Collection<Field<?>> endFields = startFields;

    if (request.getScanAfter() != null) {
      startBound = Range.Bound.EXCLUSIVE;
      startFields = getRangeFields(request, type);
    }

    Range range;
    if (request.getSortOrder() == SortOrder.ASC) {
      range = Range.create(startFields, startBound, endFields, endBound);
    } else {
      range = Range.create(endFields, endBound, startFields, startBound);
    }

    int limit = request.getLimit();
    int count = 0;
    try (CloseableIterator<StructuredRow> iterator = getScanIterator(
        table, range,
        request.getSortOrder(), request.getSortOn())
    ) {
      while (iterator.hasNext() && limit > 0) {
        StructuredRow row = iterator.next();
        SourceControlMetadataRecord record = decodeRow(row);
        boolean nameContainsFilterMatch = request.getFilter().getNameContains() == null
            || record.getName().toLowerCase()
                .contains(request.getFilter().getNameContains().toLowerCase());

        if (nameContainsFilterMatch) {
          consumer.accept(record);
          limit--;
          count++;
        }
      }
    }
    return count;
  }

  /**
   * Retrieves the source control metadata record for the specified application reference.
   *
   * @param appRef The {@link ApplicationReference} for which to retrieve the metadata record.
   * @return The {@link SourceControlMetadataRecord} associated with the specified application
   *         reference, or {@code null} if no record is found.
   * @throws IOException If an I/O error occurs while retrieving the record.
   */
  public SourceControlMetadataRecord getRecord(ApplicationReference appRef)
      throws IOException {
    List<Field<?>> primaryKey = getPrimaryKey(appRef);
    return getNamespaceSourceControlMetadataTable().read(primaryKey)
        .map(row -> decodeRow(row)
        ).orElse(null);
  }

  /**
   * Sets the source control metadata for the specified application ID in the namespace. Source
   * control metadata will be null when the application is deployed in the namespace. It will be
   * non-null when the application is pulled from the remote repository and deployed in the
   * namespace.
   *
   * @param appRef            {@link ApplicationReference} for which the source control metadata is
   *                          being set.
   * @param sourceControlMeta The {@link SourceControlMeta} to be set. Can be {@code null} if
   *                          application is just deployed.
   * @throws IOException If failed to write the data.
   */
  public void write(ApplicationReference appRef,
      SourceControlMeta sourceControlMeta)
      throws IOException, IllegalArgumentException {
    // In the Namespace Pipelines page, the sync status (SYNCED or UNSYNCED)
    // and last modified of all the applications deployed in the namespace needs to be shown.
    // If source control information is not added when the app is deployed, the data will
    // split into two tables.
    // JOIN operation is not currently supported yet. The filtering (eg,
    // filter on UNSYNCED sync status) , sorting, searching , pagination becomes difficult.
    // Instead of doing filtering, searching, sorting in memory, it will happen at
    // database level.
    StructuredTable scmTable = getNamespaceSourceControlMetadataTable();
    if (sourceControlMeta == null || sourceControlMeta.getSyncStatus() == null) {
      throw new IllegalArgumentException("Source control metadata or sync status cannot be null");
    }
    SourceControlMeta existingSourceControlMeta = get(appRef);
    if (sourceControlMeta.getLastSyncedAt() != null && existingSourceControlMeta != null
        && existingSourceControlMeta.getLastSyncedAt() != null
        && sourceControlMeta.getLastSyncedAt()
        .isBefore(existingSourceControlMeta.getLastSyncedAt())) {

      throw new IllegalArgumentException(String.format(
          "Trying to write lastSynced as %d for the app name %s in namespace %s but an "
              + "updated row having a greater last modified is already present",
          sourceControlMeta.getLastSyncedAt().toEpochMilli(),
          appRef.getEntityName(),
          appRef.getNamespaceId()));
    }
    scmTable.upsert(getAllFields(appRef, sourceControlMeta, existingSourceControlMeta));
  }

  /**
   * Deletes the source control metadata associated with the specified application ID from the
   * namespace.
   *
   * @param appRef {@link ApplicationReference} whose source control metadata is to be deleted.
   * @throws IOException if it failed to read or delete the metadata
   */
  public void delete(ApplicationReference appRef) throws IOException {
    getNamespaceSourceControlMetadataTable().delete(getPrimaryKey(appRef));
  }

  /**
   * Deletes all rows of source control metadata within the specified namespace.
   *
   * @param namespace The namespace for which all source control metadata rows are to be deleted.
   * @throws IOException if it failed to read or delete the metadata.
   */
  public void deleteAll(String namespace) throws IOException {
    getNamespaceSourceControlMetadataTable().deleteAll(getNamespaceRange(namespace));
  }

  private CloseableIterator<StructuredRow> getScanIterator(
      StructuredTable table,
      Range range,
      SortOrder sortOrder,
      SortBy sortBy) throws IOException {
    if (sortBy == SortBy.LAST_SYNCED_DATE) {
      return table.scan(range, Integer.MAX_VALUE,
          StoreDefinition.NamespaceSourceControlMetadataStore.LAST_MODIFIED_FIELD, sortOrder);
    }
    return table.scan(range, Integer.MAX_VALUE,
        StoreDefinition.NamespaceSourceControlMetadataStore.NAME_FIELD, sortOrder);
  }

  private List<Field<?>> getRangeFields(
      ScanSourceControlMetadataRequest request, String type) throws IOException {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(
        Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.NAMESPACE_FIELD,
            request.getNamespace()));
    fields.add(
        Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.TYPE_FIELD, type));
    if (request.getSortOn() == SortBy.LAST_SYNCED_DATE) {
      SourceControlMeta meta = get(
          new ApplicationReference(request.getNamespace(), request.getScanAfter()));
      fields.add(Fields.longField(
          StoreDefinition.NamespaceSourceControlMetadataStore.LAST_MODIFIED_FIELD,
          meta == null || meta.getLastSyncedAt() == null ? 0L : meta.getLastSyncedAt().toEpochMilli()));
    }
    fields.add(Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.NAME_FIELD,
        request.getScanAfter()));
    return fields;
  }

  private SourceControlMetadataRecord decodeRow(StructuredRow row) {
    String namespace = row.getString(
        StoreDefinition.NamespaceSourceControlMetadataStore.NAMESPACE_FIELD);
    String type = row.getString(StoreDefinition.NamespaceSourceControlMetadataStore.TYPE_FIELD);
    String name = row.getString(StoreDefinition.NamespaceSourceControlMetadataStore.NAME_FIELD);
    String specificationHash = row.getString(
        StoreDefinition.NamespaceSourceControlMetadataStore.SPECIFICATION_HASH_FIELD);
    String commitId = row.getString(
        StoreDefinition.NamespaceSourceControlMetadataStore.COMMIT_ID_FIELD);
    Long lastModified = row.getLong(
        StoreDefinition.NamespaceSourceControlMetadataStore.LAST_MODIFIED_FIELD);
    if (lastModified == 0L) {
      lastModified = null;
    }
    Boolean isSynced = row.getBoolean(
        StoreDefinition.NamespaceSourceControlMetadataStore.IS_SYNCED_FIELD);
    return new SourceControlMetadataRecord(namespace, type, name, specificationHash, commitId,
        lastModified, isSynced);
  }

  private Collection<Field<?>> getAllFields(ApplicationReference appRef,
      SourceControlMeta newSourceControlMeta, SourceControlMeta existingSourceControlMeta) {
    List<Field<?>> fields = getPrimaryKey(appRef);
    fields.add(Fields.stringField(
        StoreDefinition.NamespaceSourceControlMetadataStore.SPECIFICATION_HASH_FIELD,
        newSourceControlMeta.getFileHash()));
    fields.add(
        Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.COMMIT_ID_FIELD,
            newSourceControlMeta.getCommitId()));
    // Whenever an app is deployed, the expected behavior is that the last modified field will be
    // retained and not reset.
    fields.add(
        Fields.longField(StoreDefinition.NamespaceSourceControlMetadataStore.LAST_MODIFIED_FIELD,
            getLastModifiedValue(newSourceControlMeta, existingSourceControlMeta)));
    fields.add(
        Fields.booleanField(StoreDefinition.NamespaceSourceControlMetadataStore.IS_SYNCED_FIELD,
            newSourceControlMeta.getSyncStatus()));
    return fields;
  }

  private long getLastModifiedValue(SourceControlMeta newSourceControlMeta,
      SourceControlMeta existingSourceControlMeta) {
    if (newSourceControlMeta.getLastSyncedAt() == null) {
      return
          existingSourceControlMeta == null || existingSourceControlMeta.getLastSyncedAt() == null
              ? 0L : existingSourceControlMeta.getLastSyncedAt().toEpochMilli();
    }
    return newSourceControlMeta.getLastSyncedAt().toEpochMilli();
  }

  private List<Field<?>> getPrimaryKey(ApplicationReference appRef) {
    List<Field<?>> primaryKey = new ArrayList<>();
    primaryKey.add(
        Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.NAMESPACE_FIELD,
            appRef.getNamespace()));
    primaryKey.add(
        Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.TYPE_FIELD,
            EntityType.APPLICATION.toString()));
    primaryKey.add(
        Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.NAME_FIELD,
            appRef.getEntityName()));
    return primaryKey;
  }

  private Range getNamespaceRange(String namespaceId) {
    return Range.singleton(
        ImmutableList.of(
            Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, namespaceId)));
  }

  /**
   * Deletes all rows from the namespace source control metadata table. Only to be used in testing.
   *
   * @throws IOException If an I/O error occurs while deleting the metadata.
   */
  @VisibleForTesting
  void deleteNamespaceSourceControlMetadataTable() throws IOException {
    getNamespaceSourceControlMetadataTable().deleteAll(
        Range.from(ImmutableList.of(
                Fields.stringField(
                    StoreDefinition.NamespaceSourceControlMetadataStore.NAMESPACE_FIELD, "")),
            Range.Bound.INCLUSIVE));
  }

}
