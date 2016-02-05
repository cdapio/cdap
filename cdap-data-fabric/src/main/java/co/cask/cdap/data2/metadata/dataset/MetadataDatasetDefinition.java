/*
 * Copyright 2015 Cask Data, Inc.
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
package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.lib.CompositeDatasetAdmin;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.IndexedTableDefinition;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.store.DefaultMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Define the Dataset for metadata.
 */
public class MetadataDatasetDefinition extends AbstractDatasetDefinition<MetadataDataset, DatasetAdmin> {

  private static final String METADATA_INDEX_TABLE_NAME = "metadata_index";

  private final DatasetDefinition<? extends IndexedTable, ?> indexedTableDef;

  public MetadataDatasetDefinition(String name, DatasetDefinition<? extends IndexedTable, ?> indexedTableDef) {
    super(name);
    this.indexedTableDef = indexedTableDef;
  }

  // Implementation of DatasetDefinition interface methods.

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    Map<String, String> dsProperties = properties.getProperties();
    /**
     * The {@link DefaultMetadataStore#setupDatasets(DatasetFramework)} used to setup {@link MetadataDataset} during
     * upgrade passes the columns to index with old column indexed in 3.2 in {@link DatasetProperties} and the new ones
     * so if this is set then use the set value else use the index column from 3.3 defined in
     * MetadataDataset.INDEX_COLUMN.
     */
    //TODO (UPG-3.3): Remove this after 3.3
    String colToIndex;
    if (dsProperties.containsKey(IndexedTableDefinition.INDEX_COLUMNS_CONF_KEY)) {
      colToIndex = dsProperties.get(IndexedTableDefinition.INDEX_COLUMNS_CONF_KEY);
    } else {
      colToIndex = MetadataDataset.INDEX_COLUMN;
    }
    // Define the columns for indexing on the partitionsTable
    DatasetProperties indexedTableProperties = DatasetProperties.builder()
      .addAll(dsProperties)
      .add(IndexedTableDefinition.INDEX_COLUMNS_CONF_KEY, colToIndex)
      .build();
    return DatasetSpecification.builder(instanceName, getName())
      .properties(dsProperties)
      .datasets(indexedTableDef.configure(METADATA_INDEX_TABLE_NAME, indexedTableProperties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    return new CompositeDatasetAdmin(indexedTableDef.getAdmin(datasetContext,
                                                              spec.getSpecification(METADATA_INDEX_TABLE_NAME),
                                                              classLoader));
  }

  @Override
  public MetadataDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                    Map<String, String> arguments, ClassLoader classLoader) throws IOException {

    IndexedTable indexedTable  = indexedTableDef.getDataset(datasetContext,
                                                            spec.getSpecification(METADATA_INDEX_TABLE_NAME),
                                                            arguments, classLoader);

    return new MetadataDataset(indexedTable);
  }
}
