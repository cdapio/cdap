/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Dataset for namespace metadata. It does not wrap its operations in a transaction.
 * It is up to the caller to decide what operations belong in a transaction.
 */
public final class NamespaceTable {
  private static final Gson GSON = new Gson();

  private final StructuredTable table;

  public NamespaceTable(StructuredTableContext context) throws TableNotFoundException {
    this.table = context.getTable(StoreDefinition.NamespaceStore.NAMESPACES);
  }


  /**
   * Create the namespace using the namespace meta
   *
   * @param metadata the namespace metadata for the namespace
   */
  public void create(NamespaceMeta metadata) throws IOException {
    Field<String> nameField = Fields.stringField(StoreDefinition.NamespaceStore.NAMESPACE_FIELD, metadata.getName());
    Field<String> metadataField =
      Fields.stringField(StoreDefinition.NamespaceStore.NAMESPACE_METADATA_FIELD, GSON.toJson(metadata));
    table.upsert(Arrays.asList(nameField, metadataField));
  }

  /**
   * Get the namespace meta using the namespace id
   *
   * @param id id of the namespace
   * @return the namespace meta, null if not found
   */
  @Nullable
  public NamespaceMeta get(NamespaceId id) throws IOException {
    return table.read(Collections.singleton(Fields.stringField(StoreDefinition.NamespaceStore.NAMESPACE_FIELD,
                                                               id.getEntityName())))
      .map(this::getNamespaceMeta)
      .orElse(null);
  }

  /**
   * Delete the namespace from the dataset
   *
   * @param id id of the namespace
   */
  public void delete(NamespaceId id) throws IOException {
    table.delete(Collections.singleton(Fields.stringField(StoreDefinition.NamespaceStore.NAMESPACE_FIELD,
                                                          id.getEntityName())));
  }

  /**
   * List all namespaces
   *
   * @return list of all namespace metas
   */
  public List<NamespaceMeta> list() throws IOException {
    try (CloseableIterator<StructuredRow> iterator = table.scan(Range.all(), Integer.MAX_VALUE)) {
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
        .map(this::getNamespaceMeta)
        .collect(Collectors.toList());
    }
  }

  /**
   * Count all namespaces
   * @return long of all namespaces except for system.
   * @throws IOException from StructuredTable.count
   */
  public long getNamespaceCount() throws IOException {
    return getNamespaceCount(Collections.singletonList(Range.all()));
  }

  /**
   * Count namespaces for a particular range.
   * @param ranges list of ranges
   * @return long of all namespaces based on range
   * @throws IOException from StructuredTable.count
   */
  public long getNamespaceCount(Collection<Range> ranges) throws IOException {
    return table.count(ranges);
  }

  @Nullable
  private NamespaceMeta getNamespaceMeta(StructuredRow row) {
    return Optional.ofNullable(row.getString(StoreDefinition.NamespaceStore.NAMESPACE_METADATA_FIELD))
      .map(field -> GSON.fromJson(field, NamespaceMeta.class))
      .orElse(null);
  }
}
