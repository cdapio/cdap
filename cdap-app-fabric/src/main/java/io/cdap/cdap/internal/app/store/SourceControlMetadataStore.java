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
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
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
import javax.annotation.Nullable;

/**
 * Store for namespace and repository source control metadata.
 */
public class SourceControlMetadataStore {

  private StructuredTable namespaceSourceControlMetadataTable;
  private final StructuredTableContext context;

  public static SourceControlMetadataStore create(StructuredTableContext context) {
    return new SourceControlMetadataStore(context);
  }

  private SourceControlMetadataStore(StructuredTableContext context) {
    this.context = context;
  }

  private StructuredTable getNamespaceSourceControlMetadataTable() {
    try {
      if (namespaceSourceControlMetadataTable == null) {
        namespaceSourceControlMetadataTable = context.getTable(
            StoreDefinition.NamespaceSourceControlMetadataStore.NAMESPACE_SOURCE_CONTROL_METADATA);
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    return namespaceSourceControlMetadataTable;
  }

  /**
   * Retrieves the source control metadata for the specified application ID in the namespace from
   * {@code NamespaceSourceControlMetadata} table.
   *
   * @param appId {@link ApplicationId} for which the source control metadata is being retrieved.
   * @return The {@link SourceControlMeta} associated with the application ID, or {@code null} if no
   *         metadata is found.
   * @throws IOException If it fails to read the metadata.
   */
  @Nullable
  public SourceControlMeta get(ApplicationId appId) throws IOException {
    List<Field<?>> primaryKey = getPrimaryKey(appId);
    StructuredTable table = getNamespaceSourceControlMetadataTable();
    Optional<StructuredRow> row = table.read(primaryKey);

    return row.map(nonNullRow -> {
      String specificationHash = nonNullRow.getString(
          StoreDefinition.NamespaceSourceControlMetadataStore.SPECIFICATION_HASH_FIELD);
      String commitId = nonNullRow.getString(
          StoreDefinition.NamespaceSourceControlMetadataStore.COMMIT_ID_FIELD);
      Long lastSynced = nonNullRow.getLong(
          StoreDefinition.NamespaceSourceControlMetadataStore.LAST_MODIFIED_FIELD);
      if (specificationHash == null && commitId == null && lastSynced == 0L) {
        return null;
      }
      return new SourceControlMeta(specificationHash, commitId, Instant.ofEpochMilli(lastSynced));
    }).orElse(null);
  }


  /**
   * Sets the source control metadata for the specified application ID in the namespace.  Source
   * control metadata will be null when the application is deployed in the namespace. It will be
   * non-null when the application is pulled from the remote repository and deployed in the
   * namespace.
   *
   * @param appId             {@link ApplicationId} for which the source control metadata is being
   *                          set.
   * @param sourceControlMeta The {@link SourceControlMeta} to be set. Can be {@code null} if
   *                          application is just deployed.
   * @throws IOException If failed to write the data.
   */
  public void write(ApplicationId appId,
      @Nullable SourceControlMeta sourceControlMeta)
      throws IOException {
    // In the Namespace Pipelines page, the sync status (SYNCED or UNSYNCED)
    // and last modified of all the applications deployed in the namespace needs to be shown.
    // If source control information is not added when the app is deployed, the data will
    // split into two tables.
    // JOIN operation is not currently supported yet. The filtering (eg,
    // filter on UNSYNCED sync status) , sorting, searching , pagination becomes difficult.
    // Instead of doing filtering, searching, sorting in memory, it will happen at
    // database level.
    StructuredTable scmTable = getNamespaceSourceControlMetadataTable();
    scmTable.upsert(getNamespaceSourceControlMetaFields(appId, sourceControlMeta));
  }

  /**
   * Deletes the source control metadata associated with the specified application ID from the
   * namespace.
   *
   * @param appId {@link ApplicationId} whose source control metadata is to be deleted.
   * @throws IOException if it failed to read or delete the metadata
   */
  public void delete(ApplicationId appId) throws IOException {
    getNamespaceSourceControlMetadataTable().delete(getPrimaryKey(appId));
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

  private Collection<Field<?>> getNamespaceSourceControlMetaFields(ApplicationId appId,
      SourceControlMeta scmMeta) throws IOException {
    List<Field<?>> fields = getPrimaryKey(appId);
    fields.add(Fields.stringField(
        StoreDefinition.NamespaceSourceControlMetadataStore.SPECIFICATION_HASH_FIELD,
        scmMeta == null ? "" : scmMeta.getFileHash()));
    fields.add(
        Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.COMMIT_ID_FIELD,
            scmMeta == null ? "" : scmMeta.getCommitId()));
    // Whenever an app is deployed, the expected behavior is that the last modified field will be
    // retained and not reset.
    Long lastModified = 0L;
    if (scmMeta != null) {
      lastModified = scmMeta.getLastSyncedAt().toEpochMilli();
    } else {
      SourceControlMeta sourceControlMeta = get(appId);
      if (sourceControlMeta != null) {
        lastModified = sourceControlMeta.getLastSyncedAt().toEpochMilli();
      }
    }
    fields.add(
        Fields.longField(StoreDefinition.NamespaceSourceControlMetadataStore.LAST_MODIFIED_FIELD,
            lastModified));
    fields.add(
        Fields.booleanField(StoreDefinition.NamespaceSourceControlMetadataStore.IS_SYNCED_FIELD,
            scmMeta == null ? false : true));
    return fields;
  }

  private List<Field<?>> getPrimaryKey(ApplicationId appId) {
    List<Field<?>> primaryKey = new ArrayList<>();
    primaryKey.add(
        Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.NAMESPACE_FIELD,
            appId.getNamespace()));
    primaryKey.add(
        Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.TYPE_FIELD,
            appId.getEntityType().toString()));
    primaryKey.add(
        Fields.stringField(StoreDefinition.NamespaceSourceControlMetadataStore.NAME_FIELD,
            appId.getEntityName()));
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
