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

package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.queue.QueueName;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * Dataset module for datasets needed by the HBase queue.
 */
public class HBaseQueueDatasetModule implements DatasetModule {

  // The Dataset name for the consumer state store. Used in HBaseAdmin as well.
  static final String STATE_STORE_TYPE_NAME = HBaseConsumerStateStore.class.getSimpleName();
  // Runtime argument for carrying the QueueName
  static final String PROPERTY_QUEUE_NAME = "queue.name";

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<Table, DatasetAdmin> tableDef = registry.get("table");
    registry.add(new HBaseConsumerStateStoreDefinition(STATE_STORE_TYPE_NAME, tableDef));
  }

  /**
   * Dataset definition for the {@link HBaseConsumerStateStore}.
   */
  public static final class HBaseConsumerStateStoreDefinition
        extends AbstractDatasetDefinition<HBaseConsumerStateStore, DatasetAdmin> {

    private final DatasetDefinition<? extends Table, ?> tableDefinition;

    public HBaseConsumerStateStoreDefinition(String name, DatasetDefinition<? extends Table, ?> tableDefinition) {
      super(name);
      this.tableDefinition = tableDefinition;
    }

    @Override
    public DatasetSpecification configure(String name, DatasetProperties properties) {
      return DatasetSpecification.builder(name, getName())
        .properties(properties.getProperties())
        .datasets(tableDefinition.configure("config", properties))
        .build();
    }

    @Override
    public DatasetAdmin getAdmin(DatasetContext datasetContext,
                                 DatasetSpecification spec, ClassLoader classLoader) throws IOException {
      return tableDefinition.getAdmin(datasetContext, spec.getSpecification("config"), classLoader);
    }

    @Override
    public HBaseConsumerStateStore getDataset(DatasetContext datasetContext,
                                              DatasetSpecification spec,
                                              Map<String, String> arguments,
                                              ClassLoader classLoader) throws IOException {
      QueueName queueName = QueueName.from(URI.create(arguments.get(PROPERTY_QUEUE_NAME)));
      Table table = tableDefinition.getDataset(datasetContext, spec.getSpecification("config"), arguments, classLoader);
      return new HBaseConsumerStateStore(queueName, table);
    }
  }
}
