/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.store;

import com.google.gson.Gson;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Dataset for namespace repository. It does not wrap its operations in a transaction. It is up to
 * the caller to decide what operations belong in a transaction.
 */
public class RepositoryTable {

  private static final Gson GSON = new Gson();

  private final StructuredTable table;

  public RepositoryTable(StructuredTableContext context) throws TableNotFoundException {
    this.table = context.getTable(StoreDefinition.NamespaceStore.REPOSITORIES);
  }

  /**
   * Create the repository configuration
   *
   * @param id the namespace id
   * @param config the source control repository configuration for the namespace
   */
  public void create(NamespaceId id, RepositoryConfig config) throws IOException {
    Field<String> namespaceField = Fields.stringField(
        StoreDefinition.NamespaceStore.NAMESPACE_FIELD,
        id.getNamespace());
    Field<String> configField = Fields.stringField(
        StoreDefinition.NamespaceStore.REPOSITORY_CONFIGURATION_FIELD,
        GSON.toJson(config));
    Field<Long> timeField = Fields.longField(StoreDefinition.NamespaceStore.UPDATE_TIME,
        System.currentTimeMillis());
    table.upsert(Arrays.asList(namespaceField, configField, timeField));
  }

  /**
   * @param id the namespace id
   * @return {@link RepositoryMeta} the repository configuration metadata.
   */
  @Nullable
  public RepositoryMeta get(NamespaceId id) throws IOException {
    return table.read(
            Collections.singleton(Fields.stringField(StoreDefinition.NamespaceStore.NAMESPACE_FIELD,
                id.getEntityName())))
        .map(this::getRepositoryMeta)
        .orElse(null);
  }

  /**
   * Delete the repository configuration for this namespace
   *
   * @param id id of the namespace
   */
  public void delete(NamespaceId id) throws IOException {
    table.delete(
        Collections.singleton(Fields.stringField(StoreDefinition.NamespaceStore.NAMESPACE_FIELD,
            id.getEntityName())));
  }

  @Nullable
  private RepositoryMeta getRepositoryMeta(StructuredRow row) {
    RepositoryConfig config =
        Optional.ofNullable(
                row.getString(StoreDefinition.NamespaceStore.REPOSITORY_CONFIGURATION_FIELD))
            .map(field -> GSON.fromJson(field, RepositoryConfig.class))
            .orElse(null);
    long updatedTimeMillis =
        Optional.ofNullable(row.getLong(StoreDefinition.NamespaceStore.UPDATE_TIME)).orElse(0L);
    return new RepositoryMeta(config, updatedTimeMillis);
  }
}
