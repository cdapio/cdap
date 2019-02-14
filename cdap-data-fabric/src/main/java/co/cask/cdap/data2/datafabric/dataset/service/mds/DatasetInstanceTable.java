/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service.mds;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.TableNotFoundException;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.Fields;
import co.cask.cdap.spi.data.table.field.Range;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Dataset instances metadata store
 */
public final class DatasetInstanceTable {
  private static final Gson GSON = new Gson();

  private StructuredTable table;

  public DatasetInstanceTable(StructuredTableContext context) throws TableNotFoundException {
    table = context.getTable(StoreDefinition.DatasetInstanceStore.DATASET_INSTANCES);
  }

  @Nullable
  public DatasetSpecification get(DatasetId id) throws IOException {
    Optional<StructuredRow> row =
      table.read(
        ImmutableList.of(Fields.stringField(StoreDefinition.DatasetInstanceStore.NAMESPACE_FIELD,
                                            id.getNamespace()),
                         Fields.stringField(StoreDefinition.DatasetInstanceStore.DATASET_FIELD, id.getDataset())));
    if (!row.isPresent()) {
      return null;
    }
    return GSON.fromJson(
      row.get().getString(StoreDefinition.DatasetInstanceStore.DATASET_METADATA_FIELD), DatasetSpecification.class);
  }

  public void write(NamespaceId namespaceId, DatasetSpecification instanceSpec) throws IOException {
    Field<String> namespaceField = Fields.stringField(StoreDefinition.DatasetInstanceStore.NAMESPACE_FIELD,
                                                      namespaceId.getEntityName());
    Field<String> datasetField =
      Fields.stringField(StoreDefinition.DatasetInstanceStore.DATASET_FIELD, instanceSpec.getName());
    Field<String> metadataField =
      Fields.stringField(StoreDefinition.DatasetInstanceStore.DATASET_METADATA_FIELD, GSON.toJson(instanceSpec));
    table.upsert(ImmutableList.of(namespaceField, datasetField, metadataField));
  }

  public boolean delete(DatasetId datasetInstanceId) throws IOException {
    if (get(datasetInstanceId) == null) {
      return false;
    }
    table.delete(
      ImmutableList.of(Fields.stringField(StoreDefinition.DatasetInstanceStore.NAMESPACE_FIELD,
                                          datasetInstanceId.getNamespace()),
                       Fields.stringField(StoreDefinition.DatasetInstanceStore.DATASET_FIELD,
                                          datasetInstanceId.getDataset())));
    return true;
  }

  /**
   * Gets all dataset specifications in a namespace. Local workflow datasets are filtered out.
   */
  public Collection<DatasetSpecification> getAll(NamespaceId namespaceId) throws IOException {
    Predicate<DatasetSpecification> localDatasetFilter = input -> input != null
      && !Boolean.parseBoolean(input.getProperty(Constants.AppFabric.WORKFLOW_LOCAL_DATASET_PROPERTY));
    return getAll(namespaceId, localDatasetFilter);
  }

  public Collection<DatasetSpecification> get(NamespaceId namespaceId, final Map<String, String> properties)
    throws IOException {
    Predicate<DatasetSpecification> propertyFilter = input -> input != null
      && Maps.difference(properties, input.getProperties()).entriesOnlyOnLeft().isEmpty();

    return getAll(namespaceId, propertyFilter);
  }

  private Collection<DatasetSpecification> getAll(NamespaceId namespaceId, Predicate<DatasetSpecification> filter)
    throws IOException {
    Iterator<StructuredRow> iterator = table.scan(
      Range.singleton(
        ImmutableList.of(
          Fields.stringField(StoreDefinition.DatasetInstanceStore.NAMESPACE_FIELD, namespaceId.getNamespace()))),
      Integer.MAX_VALUE);
    List<DatasetSpecification> result = new ArrayList<>();
    while (iterator.hasNext()) {
      DatasetSpecification datasetSpecification =
        GSON.fromJson(
          iterator.next().getString(StoreDefinition.DatasetInstanceStore.DATASET_METADATA_FIELD),
          DatasetSpecification.class);
      if (filter.test(datasetSpecification)) {
        result.add(datasetSpecification);
      }
    }
    return result;
  }

  public Collection<DatasetSpecification> getByTypes(NamespaceId namespaceId, Set<String> typeNames)
    throws IOException {
    Predicate<DatasetSpecification> filter = input -> input != null && typeNames.contains(input.getType());
    return getAll(namespaceId, filter);
  }
}
