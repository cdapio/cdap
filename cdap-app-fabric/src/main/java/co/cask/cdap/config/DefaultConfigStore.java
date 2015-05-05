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
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
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
  private static final String PROPERTY_COLUMN = "properties";
  private static final Id.DatasetInstance configStoreDatasetInstanceId =
    Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE, Constants.ConfigStore.CONFIG_TABLE);

  private final Transactional<ConfigTable, Table> txnl;

  @Inject
  public DefaultConfigStore(CConfiguration cConf, final DatasetFramework datasetFramework,
                            TransactionExecutorFactory executorFactory) {
    txnl = Transactional.of(executorFactory, new Supplier<ConfigTable>() {
      @Override
      public ConfigTable get() {
        try {
          Table table = DatasetsUtil.getOrCreateDataset(datasetFramework, configStoreDatasetInstanceId,
                                                        "table", DatasetProperties.EMPTY,
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
    dsFramework.addInstance(Table.class.getName(), Id.DatasetInstance.from(
                              Constants.DEFAULT_NAMESPACE_ID, Joiner.on(".").join(Constants.SYSTEM_NAMESPACE,
                                                                                  Constants.ConfigStore.CONFIG_TABLE)),
                            DatasetProperties.EMPTY);
  }

  @Override
  public void create(final String namespace, final String type, final Config config) throws ConfigExistsException {
    Boolean success = txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, Boolean>() {
      @Override
      public Boolean apply(ConfigTable configTable) throws Exception {
        if (!configTable.table.get(rowKey(namespace, type, config.getId())).isEmpty()) {
          return false;
        }
        configTable.table.put(rowKey(namespace, type, config.getId()), Bytes.toBytes(PROPERTY_COLUMN),
                              Bytes.toBytes(GSON.toJson(config.getProperties())));
        return true;
      }
    });

    if (!success) {
      throw new ConfigExistsException(namespace, type, config.getId());
    }
  }

  @Override
  public void createOrUpdate(final String namespace, final String type, final Config config) {
    txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, Void>() {
      @Override
      public Void apply(ConfigTable configTable) throws Exception {
        configTable.table.put(rowKey(namespace, type, config.getId()), Bytes.toBytes(PROPERTY_COLUMN),
                              Bytes.toBytes(GSON.toJson(config.getProperties())));
        return null;
      }
    });
  }

  @Override
  public void delete(final String namespace, final String type, final String id) throws ConfigNotFoundException {
    Boolean success = txnl.executeUnchecked(new TransactionExecutor.Function<ConfigTable, Boolean>() {
      @Override
      public Boolean apply(ConfigTable configTable) throws Exception {
        if (configTable.table.get(rowKey(namespace, type, id)).isEmpty()) {
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
        Scanner rows = configTable.table.scan(prefixBytes, Bytes.stopKeyForPrefix(prefixBytes));
        Row row;
        while ((row = rows.next()) != null) {
          Map<String, String> properties = GSON.fromJson(Bytes.toString(row.get(Bytes.toBytes(PROPERTY_COLUMN))),
                                                         MAP_STRING_STRING_TYPE);
          configList.add(new Config(getPart(row.getRow(), prefixBytes.length), properties));
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
        Row row = configTable.table.get(rowKey(namespace, type, id));
        if (row.isEmpty()) {
          return null;
        }
        Map<String, String> propertyMap = GSON.fromJson(Bytes.toString(row.get(Bytes.toBytes(PROPERTY_COLUMN))),
                                                        MAP_STRING_STRING_TYPE);
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
        if (configTable.table.get(rowKey(namespace, type, config.getId())).isEmpty()) {
          return false;
        }
        configTable.table.put(rowKey(namespace, type, config.getId()), Bytes.toBytes(PROPERTY_COLUMN),
                              Bytes.toBytes(GSON.toJson(config.getProperties())));
        return true;
      }
    });

    if (!success) {
      throw new ConfigNotFoundException(namespace, type, config.getId());
    }
  }

  private byte[] rowKey(String namespace, String type, String id) {
    return getMultipartKey(namespace, type, id);
  }

  private byte[] rowKeyPrefix(String namespace, String type) {
    return getMultipartKey(namespace, type);
  }

  private String getPart(byte[] rowBytes, int offset) {
    int length = Bytes.toInt(rowBytes, offset, Bytes.SIZEOF_INT);
    return Bytes.toString(rowBytes, offset + Bytes.SIZEOF_INT, length);
  }

  private byte[] getMultipartKey(String... parts) {
    int sizeOfParts = 0;
    for (String part : parts) {
      sizeOfParts += part.length();
    }

    byte[] result = new byte[1 + sizeOfParts + (parts.length * Bytes.SIZEOF_INT)];
    Bytes.putByte(result, 0, Constants.ConfigStore.VERSION);

    int offset = 1;
    for (String part : parts) {
      Bytes.putInt(result, offset, part.length());
      offset += Bytes.SIZEOF_INT;
      Bytes.putBytes(result, offset, part.getBytes(), 0, part.length());
      offset += part.length();
    }
    return result;
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
