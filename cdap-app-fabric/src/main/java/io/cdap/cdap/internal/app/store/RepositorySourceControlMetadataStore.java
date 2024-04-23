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
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
   * Writes or updates the source control metadata for the specified repository metadata ID.
   *
   * @param appRef The {@link ApplicationReference} for which source control metadata needs to be
   *                       written or updated.
   * @param isSynced       A boolean indicating whether the entity in repository is synced with the
   *                       one in namespace
   * @param lastSynced     The timestamp of the last git pull/push of the entity
   * @throws IOException If an I/O error occurs while writing the source control metadata.
   */
  public void write(ApplicationReference appRef, Boolean isSynced,
      Long lastSynced) throws IOException {
    StructuredTable repoTable = getRepositorySourceControlMetadataTable();
    repoTable.upsert(getAllFields(appRef, isSynced, lastSynced));
  }

  /**
   * Deletes the source control metadata associated with the specified repository metadata ID.
   *
   * @param appRef The {@link ApplicationReference} for which source control metadata needs to be
   *                       deleted.
   * @throws IOException If an I/O error occurs while deleting the source control metadata.
   */
  public void delete(ApplicationReference appRef) throws IOException {
    getRepositorySourceControlMetadataTable().delete(
        getPrimaryKey(appRef));
  }

  private Collection<Field<?>> getAllFields(ApplicationReference appRef,
      Boolean isSynced, Long lastSynced) {
    List<Field<?>> fields = getPrimaryKey(appRef);
    fields.add(
        Fields.longField(StoreDefinition.RepositorySourceControlMetadataStore.LAST_MODIFIED_FIELD,
            lastSynced));
    fields.add(
        Fields.booleanField(StoreDefinition.RepositorySourceControlMetadataStore.IS_SYNCED_FIELD,
            isSynced));
    return fields;
  }

  private List<Field<?>> getPrimaryKey(ApplicationReference appRef) {
    List<Field<?>> primaryKey = new ArrayList<>();
    primaryKey.add(
        Fields.stringField(StoreDefinition.RepositorySourceControlMetadataStore.NAMESPACE_FIELD,
            appRef.getNamespace()));
    primaryKey.add(
        Fields.stringField(StoreDefinition.RepositorySourceControlMetadataStore.TYPE_FIELD,
            appRef.getEntityType().toString()));
    primaryKey.add(
        Fields.stringField(StoreDefinition.RepositorySourceControlMetadataStore.NAME_FIELD,
            appRef.getEntityName()));
    return primaryKey;
  }

  @VisibleForTesting
  ImmutablePair get(ApplicationReference appRef) throws IOException {
    List<Field<?>> primaryKey = getPrimaryKey(appRef);
    StructuredTable table = getRepositorySourceControlMetadataTable();
    return table.read(primaryKey).map(row -> {
      Long lastSynced = row.getLong(
          StoreDefinition.RepositorySourceControlMetadataStore.LAST_MODIFIED_FIELD);
      Boolean syncStatus = row.getBoolean(
          StoreDefinition.RepositorySourceControlMetadataStore.IS_SYNCED_FIELD);
      return new ImmutablePair(lastSynced, syncStatus);
    }).orElse(null);
  }
}
