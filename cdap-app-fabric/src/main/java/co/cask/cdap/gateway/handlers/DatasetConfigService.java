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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * ConfigService implementation using Dataset.
 */
public abstract class DatasetConfigService extends AbstractIdleService implements ConfigService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetConfigService.class);

  protected final DatasetFramework dsFramework;
  protected final TransactionExecutorFactory executorFactory;
  protected Table configTable;

  private TransactionExecutor executor;
  private List<TransactionAware> txAwareList;

  public DatasetConfigService(CConfiguration cConf, DatasetFramework dsFramework,
                              TransactionExecutorFactory executorFactory) {
    this.dsFramework = new NamespacedDatasetFramework(dsFramework, new DefaultDatasetNamespace(cConf,
                                                                                               Namespace.SYSTEM));
    this.executorFactory = executorFactory;
  }

  @Override
  protected void startUp() throws Exception {
    //ConfigTable - stores the configurations (as row keys) and their settings as column:values
    configTable = DatasetsUtil.getOrCreateDataset(dsFramework, Constants.ConfigService.CONFIG_STORE_TABLE,
                                                  Table.class.getName(), DatasetProperties.EMPTY, null, null);
    txAwareList = Lists.newArrayList((TransactionAware) configTable);
    executor = executorFactory.createExecutor(txAwareList);
    Preconditions.checkNotNull(configTable, "Could not get/create ConfigTable");
    Preconditions.checkNotNull(executor, "Transaction Executor is null");
  }

  protected Collection<TransactionAware> getTxAwareList() {
    return txAwareList;
  }

  @Override
  protected void shutDown() {
    closeDataSet(configTable);
  }

  protected void closeDataSet(Closeable ds) {
    try {
      ds.close();
    } catch (Throwable t) {
      LOG.error("Dataset throws exceptions during close:" + ds.toString(), t);
    }
  }

  @Override
  public void writeSetting(final String prefix, final ConfigType type, final String name, final String key,
                           final String value)
    throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        configTable.put(getRowKey(prefix, type, name), Bytes.toBytes(key), Bytes.toBytes(value));
      }
    });
  }

  @Override
  public void writeSetting(final String prefix, final ConfigType type, final String name,
                           final Map<String, String> settingsMap)
    throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (Map.Entry<String, String> setting : settingsMap.entrySet()) {
          configTable.put(getRowKey(prefix, type, name), Bytes.toBytes(setting.getKey()),
                          Bytes.toBytes(setting.getValue()));
        }
      }
    });
  }

  @Override
  public String readSetting(final String prefix, final ConfigType type, final String name, final String key)
    throws Exception {
    return executor.execute(new TransactionExecutor.Function<Object, String>() {
      @Override
      public String apply(Object o) throws Exception {
        return Bytes.toString(configTable.get(getRowKey(prefix, type, name), Bytes.toBytes(key)));
      }
    }, null);
  }

  @Override
  public String readResolvedSetting(final String prefix, final ConfigType type, final String name, final String key)
    throws Exception {
    return readSetting(prefix, type, name, key);
  }

  @Override
  public Map<String, String> readSetting(final String prefix, final ConfigType type, final String name)
    throws Exception {
    return executor.execute(new TransactionExecutor.Function<Object, Map<String, String>>() {
      @Override
      public Map<String, String> apply(Object i) throws Exception {
        Map<String, String> settings = Maps.newHashMap();
        byte[] rowKey = getRowKey(prefix, type, name);
        for (Map.Entry<byte[], byte[]> entry : configTable.get(rowKey).getColumns().entrySet()) {
          settings.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
        }
        return settings;
      }
    }, null);
  }

  @Override
  public Map<String, String> readResolvedSetting(String prefix, ConfigType type, String name) throws Exception {
    return readSetting(prefix, type, name);
  }

  @Override
  public void deleteSetting(final String prefix, final ConfigType type, final String name, final String key)
    throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        configTable.delete(getRowKey(prefix, type, name), Bytes.toBytes(key));
      }
    });
  }

  @Override
  public void deleteSetting(final String prefix, final ConfigType type, final String name) throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        configTable.delete(getRowKey(prefix, type, name));
      }
    });
  }

  @Override
  public void deleteConfig(final String prefix, final ConfigType type, final String accId, final String name)
    throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        configTable.delete(getRowKey(prefix, type, name));
      }
    });
  }

  @Override
  public String createConfig(final String prefix, final ConfigType type, final String accId) throws Exception {
    return executor.execute(new TransactionExecutor.Function<Object, String>() {
      @Override
      public String apply(Object i) throws Exception {
        return null;
      }
    }, null);
  }

  @Override
  public List<String> getConfig(final String prefix, final ConfigType type, final String accId) throws Exception {
    return ImmutableList.of();
  }

  @Override
  public List<String> getConfig(final String prefix, final ConfigType type) throws Exception {
    return ImmutableList.of();
  }

  @Override
  public boolean checkConfig(final String prefix, final ConfigType type, final String name) throws Exception {
    return true;
  }

  protected abstract String getRowKeyString(String namespace, ConfigType type, String name);

  protected byte[] getRowKey(String namespace, ConfigType type, String name) {
    return Bytes.toBytes(getRowKeyString(namespace, type, name));
  }
}
