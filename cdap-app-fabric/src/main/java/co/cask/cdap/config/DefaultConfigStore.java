/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.config;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Default Configuration Store.
 */
public class DefaultConfigStore implements ConfigStore {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultConfigStore.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final Transactional<ConfigTable, Table> txnl;

  @Inject
  public DefaultConfigStore(CConfiguration cConf, final DatasetFramework datasetFramework,
                            TransactionExecutorFactory executorFactory) {
    final DatasetFramework dsFramework = new NamespacedDatasetFramework(
      datasetFramework, new DefaultDatasetNamespace(cConf, Namespace.SYSTEM));
    txnl = Transactional.of(executorFactory, new Supplier<ConfigTable>() {
      @Override
      public ConfigTable get() {
        try {
          Table table = DatasetsUtil.getOrCreateDataset(dsFramework, Constants.ConfigStore.CONFIG_TABLE, "table",
                                                        DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
          return new ConfigTable(table);
        } catch (Exception e) {
          LOG.error("Failed to access {} table", Constants.ConfigStore.CONFIG_TABLE, e);
          throw Throwables.propagate(e);
        }
      }
    });
  }

  @Override
  public void create(final String namespace, final String type, final Config config) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, Void>() {
      @Override
      public Void apply(ConfigTable configTable) throws Exception {
        if (configTable.table.get(rowKey(namespace, type), Bytes.toBytes(config.getId())) != null) {
          throw new ConfigExistsException(namespace, type, config.getId());
        }
        configTable.table.put(rowKey(namespace, type), Bytes.toBytes(config.getId()),
                              Bytes.toBytes(GSON.toJson(config.getProperties())));
        return null;
      }
    });
  }

  @Override
  public void delete(final String namespace, final String type, final String id) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, Void>() {
      @Override
      public Void apply(ConfigTable configTable) throws Exception {
        if (configTable.table.get(rowKey(namespace, type), Bytes.toBytes(id)) == null) {
          throw new ConfigNotFoundException(namespace, type, id);
        }
        configTable.table.delete(rowKey(namespace, type), Bytes.toBytes(id));
        return null;
      }
    });
  }

  @Override
  public List<Config> list(final String namespace, final String type) throws Exception {
    return txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, List<Config>>() {
      @Override
      public List<Config> apply(ConfigTable configTable) throws Exception {
        List<Config> configList = Lists.newArrayList();
        Row row = configTable.table.get(rowKey(namespace, type));
        for (Map.Entry<byte[], byte[]> col : row.getColumns().entrySet()) {
          Map<String, String> properties = GSON.fromJson(Bytes.toString(col.getValue()), MAP_STRING_STRING_TYPE);
          configList.add(new Config(Bytes.toString(col.getKey()), properties));
        }
        return configList;
      }
    });
  }

  @Override
  public Config get(final String namespace, final String type, final String id) throws Exception {
    return txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, Config>() {
      @Override
      public Config apply(ConfigTable configTable) throws Exception {
        if (configTable.table.get(rowKey(namespace, type), Bytes.toBytes(id)) == null) {
          throw new ConfigNotFoundException(namespace, type, id);
        }
        byte[] prop = configTable.table.get(rowKey(namespace, type), Bytes.toBytes(id));
        Map<String, String> propertyMap = GSON.fromJson(Bytes.toString(prop), MAP_STRING_STRING_TYPE);
        return new Config(id, propertyMap);
      }
    });
  }

  @Override
  public void update(final String namespace, final String type, final Config config) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, Void>() {
      @Override
      public Void apply(ConfigTable configTable) throws Exception {
        if (configTable.table.get(rowKey(namespace, type), Bytes.toBytes(config.getId())) == null) {
          throw new ConfigNotFoundException(namespace, type, config.getId());
        }
        configTable.table.put(rowKey(namespace, type), Bytes.toBytes(config.getId()),
                              Bytes.toBytes(GSON.toJson(config.getProperties())));
        return null;
      }
    });
  }

  private String rowKeyString(String namespace, String type) {
    return String.format("%s.%s", namespace, type);
  }

  private byte[] rowKey(String namespace, String type) {
    return Bytes.toBytes(rowKeyString(namespace, type));
  }

  private static final class ConfigTable implements Iterable<Table> {
    private final Table table;

    private ConfigTable(Table table) {
      this.table = table;
    }

    @Override
    public Iterator<Table> iterator() {
      return Iterators.singletonIterator(table);
    }
  }
}
