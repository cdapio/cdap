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

package io.cdap.cdap.data2.registry;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Store program -> dataset/stream usage information.
 */
public class UsageTable {

  private static final Gson GSON = new Gson();
  private final StructuredTable table;

  public UsageTable(StructuredTableContext context) {
    this.table = context.getTable(StoreDefinition.UsageStore.USAGES);
  }

  @VisibleForTesting
  void deleteAll() throws IOException {
    table.deleteAll(Range.all());
  }

  /**
   * Registers usage of a dataset by a program.
   * @param programId program
   * @param datasetInstanceId dataset
   */
  public void register(ProgramId programId, DatasetId datasetInstanceId) throws IOException {
    List<Field<?>> fields = getProgramPrefix(programId);
    String datasetKey = GSON.toJson(new DatasetKey(datasetInstanceId));
    fields.add(Fields.stringField(StoreDefinition.UsageStore.DATASET_FIELD, datasetKey));
    // TODO: CDAP-14947 For whatever reason, an index cannot be a primary key...
    fields.add(Fields.stringField(StoreDefinition.UsageStore.INDEX_FIELD, datasetKey));
    table.upsert(fields);
  }

  /**
   * Unregisters all usage information of an application.
   * @param applicationId application
   */
  public void unregister(ApplicationId applicationId) throws IOException {
    List<Field<?>> fields = getApplicationPrefix(applicationId);
    table.deleteAll(Range.singleton(fields));
  }

  /**
   * Returns datasets used by a program.
   * @param programId program
   * @return datasets used by programId
   */
  public Set<DatasetId> getDatasets(ProgramId programId) throws IOException {
    return getDatasetsFromPrefix(getProgramPrefix(programId));
  }

  /**
   * Returns datasets used by an application.
   * @param applicationId application
   * @return datasets used by applicaionId
   */
  public Set<DatasetId> getDatasets(ApplicationId applicationId) throws IOException {
    return getDatasetsFromPrefix(getApplicationPrefix(applicationId));
  }

  /**
   * Returns programs using dataset.
   * @param datasetInstanceId dataset
   * @return programs using datasetInstanceId
   */
  public Set<ProgramId> getPrograms(DatasetId datasetInstanceId) throws IOException {
    Set<ProgramId> programs = new HashSet<>();
    Field<?> index = Fields.stringField(StoreDefinition.UsageStore.INDEX_FIELD,
                                        GSON.toJson(new DatasetKey(datasetInstanceId)));
    try (CloseableIterator<StructuredRow> iterator = table.scan(index)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        programs.add(new ProgramId(row.getString(StoreDefinition.UsageStore.NAMESPACE_FIELD),
                                   row.getString(StoreDefinition.UsageStore.APPLICATION_FIELD),
                                   row.getString(StoreDefinition.UsageStore.PROGRAM_TYPE_FIELD),
                                   row.getString(StoreDefinition.UsageStore.PROGRAM_FIELD)));
      }
    }
    return programs;
  }

  private Set<DatasetId> getDatasetsFromPrefix(List<Field<?>> prefix) throws IOException {
    Set<DatasetId> datasets = new HashSet<>();
    try (CloseableIterator<StructuredRow> iterator = table.scan(Range.singleton(prefix), Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        datasets.add(
          GSON.fromJson(row.getString(StoreDefinition.UsageStore.DATASET_FIELD), DatasetKey.class).getDataset());
      }
    }
    return datasets;
  }

  private static List<Field<?>> getApplicationPrefix(ApplicationId applicationId) {
    List<Field<?>> fields = new ArrayList();
    fields.add(Fields.stringField(StoreDefinition.UsageStore.NAMESPACE_FIELD, applicationId.getNamespace()));
    fields.add(Fields.stringField(StoreDefinition.UsageStore.APPLICATION_FIELD, applicationId.getApplication()));
    return fields;
  }

  private static List<Field<?>> getProgramPrefix(ProgramId programId) {
    List<Field<?>> fields = new ArrayList();
    fields.add(Fields.stringField(StoreDefinition.UsageStore.NAMESPACE_FIELD, programId.getNamespace()));
    fields.add(Fields.stringField(StoreDefinition.UsageStore.APPLICATION_FIELD, programId.getApplication()));
    fields.add(Fields.stringField(StoreDefinition.UsageStore.PROGRAM_TYPE_FIELD, programId.getType().name()));
    fields.add(Fields.stringField(StoreDefinition.UsageStore.PROGRAM_FIELD, programId.getProgram()));
    return fields;
  }

  /**
   * Key class for datasetId since we can only have one index field.
   */
  private class DatasetKey {
    private final String namespace;
    private final String datasetName;

    DatasetKey(DatasetId datasetId) {
      this.namespace = datasetId.getNamespace();
      this.datasetName = datasetId.getDataset();
    }

    public DatasetId getDataset() {
      return new DatasetId(namespace, datasetName);
    }
  }
}
