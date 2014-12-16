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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.config.ConfigService;
import co.cask.cdap.app.config.ConfigType;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * ConfigService implementation using Dataset.
 */
public class DatasetConfigService extends AbstractIdleService implements ConfigService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetConfigService.class);
  private static final String RECENT_DASHBOARD_ID = "idcount";
  private final DatasetFramework dsFramework;
  private final TransactionExecutorFactory executorFactory;
  private Table configTable;
  private Table dashboardTable;
  private KeyValueTable metaDataTable;
  private TransactionExecutor executor;

  @Inject
  public DatasetConfigService(CConfiguration cConf, DatasetFramework dsFramework,
                              TransactionExecutorFactory executorFactory) {
    this.dsFramework = new NamespacedDatasetFramework(dsFramework, new DefaultDatasetNamespace(cConf,
                                                                                               Namespace.SYSTEM));
    this.executorFactory = executorFactory;
  }

  @Override
  protected void startUp() throws Exception {
    configTable = DatasetsUtil.getOrCreateDataset(dsFramework, Constants.ConfigService.CONFIG_STORE_TABLE,
                                                  Table.class.getName(), DatasetProperties.EMPTY, null, null);
    dashboardTable = DatasetsUtil.getOrCreateDataset(dsFramework, Constants.ConfigService.DASHBOARD_OWNER_TABLE,
                                                     Table.class.getName(), DatasetProperties.EMPTY, null, null);
    metaDataTable = DatasetsUtil.getOrCreateDataset(dsFramework, Constants.ConfigService.METADATA_TABLE,
                                                    KeyValueTable.class.getName(), DatasetProperties.EMPTY, null, null);
    List<TransactionAware> txList = Lists.newArrayList();
    txList.addAll(ImmutableList.of(metaDataTable, (TransactionAware) configTable, (TransactionAware) dashboardTable));
    executor = executorFactory.createExecutor(txList);
  }

  @Override
  protected void shutDown() throws Exception {
    dashboardTable.close();
    configTable.close();
    metaDataTable.close();
  }

  @Override
  public void writeSetting(final ConfigType type, final String name, final String key, final String value)
    throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        configTable.put(getRowKey(type, name), Bytes.toBytes(key), Bytes.toBytes(value));
      }
    });
  }

  @Override
  public void writeSetting(final ConfigType type, final String name, final Map<String, String> settingsMap)
    throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (Map.Entry<String, String> setting : settingsMap.entrySet()) {
          configTable.put(getRowKey(type, name), Bytes.toBytes(setting.getKey()), Bytes.toBytes(setting.getValue()));
        }
      }
    });
  }

  @Override
  public String readSetting(final ConfigType type, final String name, final String key) throws Exception {
    return executor.execute(new TransactionExecutor.Function<Object, String>() {
      @Override
      public String apply(Object o) throws Exception {
        return Bytes.toString(configTable.get(getRowKey(type, name), Bytes.toBytes(key)));
      }
    }, null);
  }

  @Override
  public Map<String, String> readSetting(final ConfigType type, final String name) throws Exception {
    return executor.execute(new TransactionExecutor.Function<Object, Map<String, String>>() {
      @Override
      public Map<String, String> apply(Object i) throws Exception {
        Map<String, String> settings = Maps.newHashMap();
        for (Map.Entry<byte[], byte[]> entry : configTable.get(getRowKey(type, name)).getColumns().entrySet()) {
          settings.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
        }
        return settings;
      }
    }, null);
  }

  @Override
  public void deleteSetting(final ConfigType type, final String name, final String key) throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        configTable.delete(getRowKey(type, name), Bytes.toBytes(key));
      }
    });
  }

  @Override
  public void deleteConfig(final ConfigType type, final String accId, final String name) throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        configTable.delete(getRowKey(type, name));
        if (type == ConfigType.DASHBOARD) {
          dashboardTable.delete(Bytes.toBytes(accId), Bytes.toBytes(name));
        }
      }
    });
  }

  @Override
  public String createConfig(final ConfigType type, final String accId) throws Exception {
    return executor.execute(new TransactionExecutor.Function<Object, String>() {
      @Override
      public String apply(Object i) throws Exception {
        if (type != ConfigType.DASHBOARD) {
          return null;
        }
        byte[] value = metaDataTable.read(RECENT_DASHBOARD_ID);
        Long id =  (value == null) ? 0 : (Bytes.toLong(value) + 1);
        String dashboardId = Long.toString(id);
        dashboardTable.put(Bytes.toBytes(accId), Bytes.toBytes(dashboardId), Bytes.toBytes(true));
        metaDataTable.write(RECENT_DASHBOARD_ID, Bytes.toBytes(id));
        return dashboardId;
      }
    }, null);
  }

  @Override
  public List<String> getConfig(final ConfigType type, final String accId) throws Exception {
    List<String> configs = Lists.newArrayList();
    if (type == ConfigType.USER) {
      configs.add(accId);
    } else if (type == ConfigType.DASHBOARD) {
      for (Map.Entry<byte[], byte[]> entry : dashboardTable.get(Bytes.toBytes(accId)).getColumns().entrySet()) {
        configs.add(Bytes.toString(entry.getKey()));
      }
    }
    return configs;
  }

  @Override
  public List<String> getConfig(final ConfigType type) throws Exception {
    List<String> configs = Lists.newArrayList();
    byte[] startRowPrefix = getRowKey(type, "");
    byte[] endRowPrefix = Bytes.stopKeyForPrefix(startRowPrefix);
    Scanner scanner = configTable.scan(startRowPrefix, endRowPrefix);
    Row row;
    while ((row = scanner.next()) != null) {
      String rowKey = Bytes.toString(row.getRow());
      configs.add(rowKey.substring(rowKey.indexOf('.') + 1));
    }
    return configs;
  }

  @Override
  public boolean checkConfig(final ConfigType type, final String name) throws Exception {
    return executor.execute(new TransactionExecutor.Function<Object, Boolean>() {
      @Override
      public Boolean apply(Object i) throws Exception {
        Row row = configTable.get(getRowKey(type, name));
        return (row.getRow() != null);
      }
    }, null);
  }

  private byte[] getRowKey(ConfigType type, String name) {
    String rowKeyString = null;
    if (ConfigType.DASHBOARD == type) {
      rowKeyString = String.format("dashboard.%s", name);
    } else if (ConfigType.USER == type) {
      rowKeyString = String.format("user.%s", name);
    }
    return Bytes.toBytes(rowKeyString);
  }
}
