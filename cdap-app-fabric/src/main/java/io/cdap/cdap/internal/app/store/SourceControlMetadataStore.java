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

import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

public class SourceControlMetadataStore {
  private StructuredTable sourceControlMetadataTable;
  private final StructuredTableContext context;

  public static SourceControlMetadataStore create(StructuredTableContext context) {
    return new SourceControlMetadataStore(context);
  }

  private SourceControlMetadataStore(StructuredTableContext context) {
    this.context = context;
  }

  private StructuredTable getSourceControlMetadataTable() {
    try {
      if (sourceControlMetadataTable == null) {
        sourceControlMetadataTable = context.getTable(
            StoreDefinition.SourceControlMetadataStore.SOURCE_CONTROL_METADATA);
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    return sourceControlMetadataTable;
  }

  public void updateAppScmMeta(ApplicationId appId, SourceControlMeta scmMeta)
      throws IOException, ApplicationNotFoundException {
    StructuredTable scmTable = getSourceControlMetadataTable();
    List<Field<?>> fields = getPrimaryKey(appId);
    Optional<StructuredRow> existing = scmTable.read(fields);
    if (!existing.isPresent()) {
      throw new ApplicationNotFoundException(appId);
    }
    fields.add(Fields.stringField(StoreDefinition.SourceControlMetadataStore.SPECIFICATION_HASH_FIELD, scmMeta.getFileHash()));
    fields.add(Fields.stringField(StoreDefinition.SourceControlMetadataStore.COMMIT_ID_FIELD, scmMeta.getCommitId()));
    fields.add(Fields.longField(StoreDefinition.SourceControlMetadataStore.LAST_MODIFIED_FIELD, scmMeta.getLastSyncedAt().toEpochMilli()));
    scmTable.upsert(fields);
  }

  public void setAppSourceControlMeta(ApplicationId appId, SourceControlMeta sourceControlMeta)
      throws IOException {
    StructuredTable scmTable = getSourceControlMetadataTable();
    scmTable.upsert(getSourceControlMetaFields(appId, sourceControlMeta, sourceControlMeta == null ? false: true));
  }

  @Nullable
  public SourceControlMeta getAppSourceControlMeta(ApplicationReference appRef) throws IOException {
    List<Field<?>> primaryKey = getPrimaryKey(
        appRef.getNamespace(), appRef.getEntityType().toString(), appRef.getEntityName());
    Optional<StructuredRow> row = getSourceControlMetadataTable().read(primaryKey);
    if (row.isPresent()) {
      StructuredRow nonNullRow = row.get();
      String specificationHash = nonNullRow.getString(StoreDefinition.SourceControlMetadataStore.SPECIFICATION_HASH_FIELD);
      String commitId = nonNullRow.getString(StoreDefinition.SourceControlMetadataStore.COMMIT_ID_FIELD);
      Long lastSynced = nonNullRow.getLong(StoreDefinition.SourceControlMetadataStore.LAST_MODIFIED_FIELD);
      return new SourceControlMeta(specificationHash, commitId, Instant.ofEpochMilli(lastSynced));
    }
    return null;
  }

  private Collection<Field<?>> getSourceControlMetaFields(ApplicationId appId,
      SourceControlMeta scmMeta, Boolean isSynced) {
    List<Field<?>> fields = getPrimaryKey(appId);
    fields.add(Fields.stringField(StoreDefinition.SourceControlMetadataStore.SPECIFICATION_HASH_FIELD, scmMeta.getFileHash()));
    fields.add(Fields.stringField(StoreDefinition.SourceControlMetadataStore.COMMIT_ID_FIELD, scmMeta.getCommitId()));
    fields.add(Fields.longField(StoreDefinition.SourceControlMetadataStore.LAST_MODIFIED_FIELD, scmMeta.getLastSyncedAt().toEpochMilli()));
    fields.add(Fields.booleanField(StoreDefinition.SourceControlMetadataStore.IS_SYNCED_FIELD, isSynced));
    return fields;
  }

  private List<Field<?>> getPrimaryKey(ApplicationId appId) {
    return getPrimaryKey(appId.getNamespace(), appId.getEntityType().toString(),
        appId.getEntityName());
  }

  private List<Field<?>> getPrimaryKey(String namespace, String type, String name) {
    List<Field<?>> primaryKey = new ArrayList<>();
    primaryKey.add(
        Fields.stringField(StoreDefinition.SourceControlMetadataStore.NAMESPACE_FIELD, namespace));
    primaryKey.add(Fields.stringField(StoreDefinition.SourceControlMetadataStore.TYPE_FIELD, type));
    primaryKey.add(Fields.stringField(StoreDefinition.SourceControlMetadataStore.NAME_FIELD, name));
    return primaryKey;
  }
}
