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

package co.cask.cdap.config;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
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

import java.io.IOException;
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

  private final Transactional<ConfigTable, KeyValueTable> txnl;

  @Inject
  public DefaultConfigStore(CConfiguration cConf, final DatasetFramework datasetFramework,
                            TransactionExecutorFactory executorFactory) {
    final DatasetFramework dsFramework = new NamespacedDatasetFramework(
      datasetFramework, new DefaultDatasetNamespace(cConf, Namespace.SYSTEM));
    txnl = Transactional.of(executorFactory, new Supplier<ConfigTable>() {
      @Override
      public ConfigTable get() {
        try {
          KeyValueTable table = DatasetsUtil.getOrCreateDataset(dsFramework, Constants.ConfigStore.CONFIG_TABLE,
                                                                "keyValueTable", DatasetProperties.EMPTY,
                                                                DatasetDefinition.NO_ARGUMENTS, null);
          return new ConfigTable(table);
        } catch (Exception e) {
          LOG.error("Failed to access {} table", Constants.ConfigStore.CONFIG_TABLE, e);
          throw Throwables.propagate(e);
        }
      }
    });
  }

  public static void setupDatasets(DatasetFramework dsFramework) throws DatasetManagementException, IOException {
    dsFramework.addInstance(KeyValueTable.class.getName(), Constants.ConfigStore.CONFIG_TABLE, DatasetProperties.EMPTY);
  }

  @Override
  public void create(final String namespace, final String type, final Config config) throws ConfigExistsException {
    Boolean success = txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, Boolean>() {
      @Override
      public Boolean apply(ConfigTable configTable) throws Exception {
        if (configTable.table.read(rowKey(namespace, type, config.getId())) != null) {
          return false;
        }
        configTable.table.write(rowKey(namespace, type, config.getId()),
                                Bytes.toBytes(GSON.toJson(config.getProperties())));
        return true;
      }
    });

    if (!success) {
      throw new ConfigExistsException(namespace, type, config.getId());
    }
  }

  @Override
  public void delete(final String namespace, final String type, final String id) throws ConfigNotFoundException {
    Boolean success = txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, Boolean>() {
      @Override
      public Boolean apply(ConfigTable configTable) throws Exception {
        if (configTable.table.read(rowKey(namespace, type, id)) == null) {
          return false;
        }
        configTable.table.delete(rowKey(namespace, type, id));
        return true;
      }
    });

    if (!success) {
      throw new ConfigNotFoundException(namespace, type, id);
    }
  }

  @Override
  public List<Config> list(final String namespace, final String type) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, List<Config>>() {
      @Override
      public List<Config> apply(ConfigTable configTable) throws Exception {
        List<Config> configList = Lists.newArrayList();
        byte[] prefixBytes = rowKeyPrefix(namespace, type);
        CloseableIterator<KeyValue<byte[], byte[]>> rows = configTable.table.scan(prefixBytes,
                                                                                  Bytes.stopKeyForPrefix(prefixBytes));
        while (rows.hasNext()) {
          KeyValue<byte[], byte[]> row = rows.next();
          Map<String, String> properties = GSON.fromJson(Bytes.toString(row.getValue()), MAP_STRING_STRING_TYPE);
          configList.add(new Config(getId(row.getKey(), prefixBytes), properties));
        }
        return configList;
      }
    });
  }

  @Override
  public Config get(final String namespace, final String type, final String id) throws ConfigNotFoundException {
    Config config = txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, Config>() {
      @Override
      public Config apply(ConfigTable configTable) throws Exception {
        byte[] prop = configTable.table.read(rowKey(namespace, type, id));
        if (prop == null) {
          return null;
        }
        Map<String, String> propertyMap = GSON.fromJson(Bytes.toString(prop), MAP_STRING_STRING_TYPE);
        return new Config(id, propertyMap);
      }
    });

    if (config == null) {
      throw new ConfigNotFoundException(namespace, type, id);
    } else {
      return config;
    }
  }

  @Override
  public void update(final String namespace, final String type, final Config config) throws ConfigNotFoundException {
    Boolean success = txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, Boolean>() {
      @Override
      public Boolean apply(ConfigTable configTable) throws Exception {
        if (configTable.table.read(rowKey(namespace, type, config.getId())) == null) {
          return false;
        }
        configTable.table.write(rowKey(namespace, type, config.getId()),
                                Bytes.toBytes(GSON.toJson(config.getProperties())));
        return true;
      }
    });

    if (!success) {
      throw new ConfigNotFoundException(namespace, type, config.getId());
    }
  }

  private String rowKeyString(String namespace, String type, String id) {
    return String.format("%s%04d%s", rowKeyPrefixString(namespace, type), id.length(), id);
  }

  private byte[] rowKey(String namespace, String type, String id) {
    return Bytes.toBytes(rowKeyString(namespace, type, id));
  }

  private String rowKeyPrefixString(String namespace, String type) {
    int nsSize = namespace.length();
    int typeSize = type.length();
    return String.format("%01x%04d%s%04d%s", Constants.ConfigStore.VERSION, nsSize, namespace, typeSize, type);
  }

  private String getId(byte[] rowBytes, byte[] prefixBytes) {
    int idSize = Integer.valueOf(Bytes.toString(rowBytes, prefixBytes.length, Bytes.SIZEOF_INT));
    return Bytes.toString(rowBytes, prefixBytes.length + Bytes.SIZEOF_INT, idSize);
  }

  private byte[] rowKeyPrefix(String namespace, String type) {
    return Bytes.toBytes(rowKeyPrefixString(namespace, type));
  }

  private static final class ConfigTable implements Iterable<KeyValueTable> {
    private final KeyValueTable table;

    private ConfigTable(KeyValueTable table) {
      this.table = table;
    }

    @Override
    public Iterator<KeyValueTable> iterator() {
      return Iterators.singletonIterator(table);
    }
  }
}
