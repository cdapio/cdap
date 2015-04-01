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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.queue.QueueConstants;

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
  // State store table name is system.queue.
  // The embedded table used in HBaseConsumerStateStore has the name "config", hence it will
  // map to "system.queue.config" for backward compatibility
  static final String STATE_STORE_NAME
    = Constants.SYSTEM_NAMESPACE + "." + QueueConstants.QueueType.QUEUE.toString();
  // The name of the embedded table dataset for the state store. It has to be "config".
  static final String STATE_STORE_EMBEDDED_TABLE_NAME = "config";

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
        .datasets(tableDefinition.configure(STATE_STORE_EMBEDDED_TABLE_NAME, properties))
        .build();
    }

    @Override
    public DatasetAdmin getAdmin(DatasetContext datasetContext,
                                 DatasetSpecification spec, ClassLoader classLoader) throws IOException {
      return tableDefinition.getAdmin(datasetContext, spec.getSpecification(STATE_STORE_EMBEDDED_TABLE_NAME),
                                      classLoader);
    }

    @Override
    public HBaseConsumerStateStore getDataset(DatasetContext datasetContext,
                                              DatasetSpecification spec,
                                              Map<String, String> arguments,
                                              ClassLoader classLoader) throws IOException {
      QueueName queueName = QueueName.from(URI.create(arguments.get(PROPERTY_QUEUE_NAME)));
      Table table = tableDefinition.getDataset(datasetContext, spec.getSpecification(STATE_STORE_EMBEDDED_TABLE_NAME),
                                               arguments, classLoader);
      return new HBaseConsumerStateStore(spec.getName(), queueName, table);
    }
  }
}
