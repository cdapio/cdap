/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.explore.service.datasets;

import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Table;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * A wrapper around the Table dataset. You wouldn't actually do this, purely for testing purposes.
 */
public class TableWrapperDefinition extends
  AbstractDatasetDefinition<TableWrapperDefinition.TableWrapper, DatasetAdmin> {

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public TableWrapperDefinition(String name,
                                DatasetDefinition<? extends Table, ?> orderedTableDefinition) {
    super(name);
    this.tableDef = orderedTableDefinition;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
     .datasets(tableDef.configure("table", properties))
     .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(datasetContext, spec.getSpecification("table"), classLoader);
  }

  @Override
  public TableWrapper getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                        Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    Table table = tableDef.getDataset(datasetContext, spec.getSpecification("table"), arguments, classLoader);
    return new TableWrapper(spec.getName(), table);
  }

  /**
   * Doesn't actually do anything, only used to test hive table creation.
   */
  public static class TableWrapper extends AbstractDataset
    implements RecordScannable<StructuredRecord>, RecordWritable<StructuredRecord> {

    public TableWrapper(String instanceName, Table table) {
      super(instanceName, table);
    }

    @Override
    public Type getRecordType() {
      return StructuredRecord.class;
    }

    @Override
    public void write(StructuredRecord structuredRecord) throws IOException {

    }

    @Override
    public List<Split> getSplits() {
      return null;
    }

    @Override
    public RecordScanner<StructuredRecord> createSplitRecordScanner(Split split) {
      return null;
    }
  }

  /**
   */
  public static class Module implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> tableDef = registry.get("table");
      registry.add(new TableWrapperDefinition("TableWrapper", tableDef));
    }
  }

}

