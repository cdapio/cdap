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
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.proto.SourceControlMetadataRecord;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.sourcecontrol.SortBy;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Store for repository source control metadata.
 */
public class RepositorySourceControlMetadataStore {

  private StructuredTable repositorySourceControlMetadataTable;
  private final StructuredTableContext context;

  public static RepositorySourceControlMetadataStore create(StructuredTableContext context) {
    return new RepositorySourceControlMetadataStore(context);
  }

  private RepositorySourceControlMetadataStore(StructuredTableContext context) {
    this.context = context;
  }

  private StructuredTable getRepositorySourceControlMetadataTable() {
    try {
      if (repositorySourceControlMetadataTable == null) {
        repositorySourceControlMetadataTable = context.getTable(
            StoreDefinition.RepositorySourceControlMetadataStore.REPOSITORY_SOURCE_CONTROL_METADATA);
      }
    } catch (TableNotFoundException e) {
      throw new TableNotFoundException(
          StoreDefinition.RepositorySourceControlMetadataStore.REPOSITORY_SOURCE_CONTROL_METADATA);
    }
    return repositorySourceControlMetadataTable;
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

    startFields.add(
        Fields.stringField(StoreDefinition.RepositorySourceControlMetadataStore.NAMESPACE_FIELD,
            request.getNamespace())
    );
    startFields.add(
        Fields.stringField(StoreDefinition.RepositorySourceControlMetadataStore.TYPE_FIELD,
            type)
    );

    if (request.getFilter().getIsSynced() != null) {
      startFields.add(
          Fields.booleanField(StoreDefinition.RepositorySourceControlMetadataStore.IS_SYNCED_FIELD,
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

    StructuredTable table = getRepositorySourceControlMetadataTable();
    int limit = request.getLimit();
    int count = 0;
    try (CloseableIterator<StructuredRow> iterator = getScanIterator(table,
        range,
        request.getSortOrder(), request.getSortOn())
    ) {
      while (iterator.hasNext() && limit > 0) {
        StructuredRow row = iterator.next();
        SourceControlMetadataRecord record = decodeRow(row);
        boolean nameContainsFilterMatch =
            request.getFilter().getNameContains() == null || record.getName().toLowerCase()
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
   * Writes or updates the source control metadata for the specified repository metadata ID.
   *
   * @param appRef     The {@link ApplicationReference} for which source control metadata needs to
   *                   be written or updated.
   * @param isSynced   A boolean indicating whether the entity in repository is synced with the one
   *                   in namespace
   * @param lastSynced The timestamp of the last git pull/push of the entity
   * @throws IOException If an I/O error occurs while writing the source control metadata.
   */
  public void write(ApplicationReference appRef, boolean isSynced,
      Long lastSynced) throws IOException {
    StructuredTable repoTable = getRepositorySourceControlMetadataTable();
    repoTable.upsert(getAllFields(appRef, isSynced, lastSynced));
  }

  /**
   * Writes the sync status for the specified application reference.
   *
   * @param appRef   The application reference for which synchronization status is to be written.
   * @param isSynced A boolean indicating whether the application is synced or not.
   * @throws IOException If an I/O error occurs while writing the synchronization status.
   */
  public void writeSyncStatus(ApplicationReference appRef, boolean isSynced) throws IOException {
    SourceControlMetadataRecord record = get(appRef);
    write(appRef, isSynced, record.getLastModified());
  }

  /**
   * Deletes the source control metadata associated with the specified repository metadata ID.
   *
   * @param appRef The {@link ApplicationReference} for which source control metadata needs to be
   *               deleted.
   * @throws IOException If an I/O error occurs while deleting the source control metadata.
   */
  public void delete(ApplicationReference appRef) throws IOException {
    getRepositorySourceControlMetadataTable().delete(
        getPrimaryKey(appRef));
  }

  private CloseableIterator<StructuredRow> getScanIterator(
      StructuredTable table,
      Range range,
      SortOrder sortOrder,
      SortBy sortBy) throws IOException {
    if (sortBy == SortBy.LAST_SYNCED_AT) {
      return table.scan(range, Integer.MAX_VALUE,
          StoreDefinition.RepositorySourceControlMetadataStore.LAST_MODIFIED_FIELD, sortOrder);
    }
    return table.scan(range, Integer.MAX_VALUE,
        StoreDefinition.RepositorySourceControlMetadataStore.NAME_FIELD, sortOrder);
  }

  private SourceControlMetadataRecord decodeRow(StructuredRow row) {
    String namespace = row.getString(
        StoreDefinition.RepositorySourceControlMetadataStore.NAMESPACE_FIELD);
    String type = row.getString(StoreDefinition.RepositorySourceControlMetadataStore.TYPE_FIELD);
    String name = row.getString(StoreDefinition.RepositorySourceControlMetadataStore.NAME_FIELD);
    Long lastModified = row.getLong(
        StoreDefinition.RepositorySourceControlMetadataStore.LAST_MODIFIED_FIELD);
    if (lastModified == 0L) {
      lastModified = null;
    }
    Boolean isSynced = row.getBoolean(
        StoreDefinition.RepositorySourceControlMetadataStore.IS_SYNCED_FIELD);
    return new SourceControlMetadataRecord(namespace, type, name, null, null,
        lastModified, isSynced);
  }

  private Collection<Field<?>> getAllFields(ApplicationReference appRef,
      Boolean isSynced, Long lastSynced) {
    List<Field<?>> fields = getPrimaryKey(appRef);
    fields.add(
        Fields.longField(StoreDefinition.RepositorySourceControlMetadataStore.LAST_MODIFIED_FIELD,
            lastSynced == null ? 0L : lastSynced));
    fields.add(
        Fields.booleanField(StoreDefinition.RepositorySourceControlMetadataStore.IS_SYNCED_FIELD,
            isSynced));
    return fields;
  }

  private List<Field<?>> getRangeFields(ScanSourceControlMetadataRequest request,
      String type) throws IOException {
    List<Field<?>> fields = new ArrayList<>();

    fields.add(
        Fields.stringField(StoreDefinition.RepositorySourceControlMetadataStore.NAMESPACE_FIELD,
            request.getNamespace()));
    fields.add(
        Fields.stringField(StoreDefinition.RepositorySourceControlMetadataStore.TYPE_FIELD, type));
    if (request.getSortOn() == SortBy.LAST_SYNCED_AT) {
      SourceControlMetadataRecord record = get(
          new ApplicationReference(request.getNamespace(), request.getScanAfter()));
      fields.add(Fields.longField(
          StoreDefinition.RepositorySourceControlMetadataStore.LAST_MODIFIED_FIELD,
          record.getLastModified() == null ? 0L : record.getLastModified()));
    }
    fields.add(Fields.stringField(StoreDefinition.RepositorySourceControlMetadataStore.NAME_FIELD,
        request.getScanAfter()));
    return fields;
  }

  private List<Field<?>> getPrimaryKey(ApplicationReference appRef) {
    List<Field<?>> primaryKey = new ArrayList<>();
    primaryKey.add(
        Fields.stringField(StoreDefinition.RepositorySourceControlMetadataStore.NAMESPACE_FIELD,
            appRef.getNamespace()));
    primaryKey.add(
        Fields.stringField(StoreDefinition.RepositorySourceControlMetadataStore.TYPE_FIELD,
            EntityType.APPLICATION.toString()));
    primaryKey.add(
        Fields.stringField(StoreDefinition.RepositorySourceControlMetadataStore.NAME_FIELD,
            appRef.getEntityName()));
    return primaryKey;
  }

  /**
   * Retrieves sync status and last modified for the specified {@link ApplicationReference}.
   *
   * @param appRef The application reference to retrieve metadata for.
   * @return An {@link ImmutablePair} containing the lastSynced timestamp and sync status,
   *         or null if metadata is not found.
   * @throws IOException If an I/O error occurs.
   */
  public SourceControlMetadataRecord get(ApplicationReference appRef) throws IOException {
    List<Field<?>> primaryKey = getPrimaryKey(appRef);
    StructuredTable table = getRepositorySourceControlMetadataTable();
    return table.read(primaryKey).map(this::decodeRow).orElse(null);
  }

  /**
   * Deletes all repository source control metadata records for a given namespace.
   *
   * @param namespace The namespace for which to delete all repository source control metadata
   *                  records.
   * @throws IOException If an I/O error occurs during the deletion process.
   */
  @VisibleForTesting
  public void deleteAll(String namespace) throws IOException {
    getRepositorySourceControlMetadataTable().deleteAll(Range.singleton(
        ImmutableList.of(
            Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, namespace))));
  }

  /**
   * Retrieves all source control metadata records for the specified namespace and type.
   *
   * @param namespace The namespace to retrieve records from.
   * @param type      The type of records to retrieve.
   * @return A list of metadata records.
   * @throws IOException If an I/O error occurs.
   */
  @VisibleForTesting
  public List<SourceControlMetadataRecord> getAll(String namespace, String type)
      throws IOException {
    ScanSourceControlMetadataRequest request = ScanSourceControlMetadataRequest.builder()
        .setNamespace(namespace).build();
    List<SourceControlMetadataRecord> records = new ArrayList<>();
    scan(request, type, records::add);
    return records;
  }
}
