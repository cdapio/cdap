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
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.config.ConfigType;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Dashboard ConfigService.
 */
public class DashboardConfigService extends DatasetConfigService {
  private static final Logger LOG = LoggerFactory.getLogger(DashboardConfigService.class);
  private static final String RECENT_DASHBOARD_ID = "idcount";

  private final TransactionExecutorFactory executorFactory;
  private TransactionExecutor executor;
  private Table dashboardTable;
  private KeyValueTable metaDataTable;

  @Inject
  public DashboardConfigService(CConfiguration cConf, DatasetFramework dsFramework, TransactionExecutorFactory
    executorFactory) {
    super(cConf, dsFramework, executorFactory);
    this.executorFactory = executorFactory;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting DashboardConfigService...");
    super.startUp();
    //DashboardTable - stores the users (as row key) and the dashboards created by them as columns.
    dashboardTable = DatasetsUtil.getOrCreateDataset(dsFramework, Constants.ConfigService.DASHBOARD_OWNER_TABLE,
                                                     Table.class.getName(), DatasetProperties.EMPTY, null, null);
    //MetaDataTable - stores monotonically increasing count for dashboard-id of each namespace.
    //It also stores the dashboard configurations as rows (created when a dashboard is created and deleted when the
    //dashboard is deleted.
    metaDataTable = DatasetsUtil.getOrCreateDataset(dsFramework, Constants.ConfigService.METADATA_TABLE,
                                                    KeyValueTable.class.getName(), DatasetProperties.EMPTY, null, null);
    Preconditions.checkNotNull(dashboardTable, "Could not get/create DashboardTable");
    Preconditions.checkNotNull(metaDataTable, "Could not get/create MetaDataTable");
    List<TransactionAware> txList = Lists.newArrayList();
    txList.addAll(getTxAwareList());
    txList.add((TransactionAware) dashboardTable);
    txList.add((TransactionAware) metaDataTable);
    executor = executorFactory.createExecutor(txList);
    LOG.info("DashboardConfigService started...");
  }

  @Override
  protected void shutDown() {
    closeDataSet(dashboardTable);
    closeDataSet(metaDataTable);
  }

  @Override
  public String readResolvedSetting(final String prefix, final ConfigType type, final String name, final String key)
    throws Exception {
    return executor.execute(new TransactionExecutor.Function<Void, String>() {
      @Override
      public String apply(Void i) throws Exception {
        String rowKey = getRowKeyString(prefix, type, name);
        return null;
      }
    }, null);
  }

  @Override
  public Map<String, String> readResolvedSetting(String prefix, ConfigType type, String name) throws Exception {
    return null;
  }

  @Override
  public void deleteConfig(final String prefix, final ConfigType type, final String accId, final String name)
    throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        configTable.delete(getRowKey(prefix, type, name));
        //Delete the dashboard entry from the dashboard ownership table and the metaDataTable
        dashboardTable.delete(Bytes.toBytes(accId), getRowKey(prefix, type, name));
        metaDataTable.delete(getRowKey(prefix, type, name));
      }
    });
  }

  @Override
  public String createConfig(final String prefix, final ConfigType type, final String accId) throws Exception {
    final String countId = String.format("namespace.%s.%s", prefix, RECENT_DASHBOARD_ID);
    return executor.execute(new TransactionExecutor.Function<Object, String>() {
      @Override
      public String apply(Object i) throws Exception {
        byte[] value = metaDataTable.read(countId);
        //Get the next id for the given namespace
        Long id =  (value == null) ? 0 : (Bytes.toLong(value) + 1);
        String dashboardId = Long.toString(id);
        //Add the dashboard id to the list of dashboards owned by the user
        dashboardTable.put(Bytes.toBytes(accId), getRowKey(prefix, type, dashboardId), Bytes.toBytes(true));
        //Update the latest dashboard-id for the given namespace
        metaDataTable.write(countId, Bytes.toBytes(id));
        //Make an entry in the metaDataTable to indicate the creation of this dashboard
        metaDataTable.write(getRowKey(prefix, type, dashboardId), Bytes.toBytes(true));
        return dashboardId;
      }
    }, null);
  }

  @Override
  public List<String> getConfig(final String prefix, final ConfigType type, final String accId) throws Exception {
    final List<String> configs = Lists.newArrayList();
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        //Get all the dashboards owned by the user
        for (Map.Entry<byte[], byte[]> entry : dashboardTable.get(Bytes.toBytes(accId)).getColumns().entrySet()) {
          String column = Bytes.toString(entry.getKey());
          //Select only the ones that belong to the given namespace
          if (column.startsWith(getRowKeyString(prefix, type, null))) {
            //Extract the dashboard id
            configs.add(column.substring(column.lastIndexOf(".") + 1));
          }
        }
      }
    });
    return configs;
  }

  @Override
  public List<String> getConfig(final String prefix, final ConfigType type) throws Exception {
    final List<String> configs = com.google.common.collect.Lists.newArrayList();
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        //Scan the metaDataTable for rows that have keys corresponding to the given namespace
        byte[] startRowPrefix = getRowKey(prefix, type, null);
        byte[] endRowPrefix = Bytes.stopKeyForPrefix(startRowPrefix);
        CloseableIterator<KeyValue<byte[], byte[]>> iterator = metaDataTable.scan(startRowPrefix, endRowPrefix);
        while (iterator.hasNext()) {
          KeyValue<byte[], byte[]> entry = iterator.next();
          String rowKey = Bytes.toString(entry.getKey());
          //Extract the dashboard id
          configs.add(rowKey.substring(rowKey.lastIndexOf('.') + 1));
        }
        iterator.close();

      }
    });
    return configs;
  }

  @Override
  public boolean checkConfig(final String prefix, final ConfigType type, final String name) throws Exception {
    return executor.execute(new TransactionExecutor.Function<Object, Boolean>() {
      @Override
      public Boolean apply(Object i) throws Exception {
        byte[] value = (metaDataTable.read(getRowKey(prefix, type, name)));
        return (value != null);
      }
    }, null);
  }

  protected String getRowKeyString(String namespace, ConfigType type, String name) {
    String rowKeyString = String.format("namespace.%s.dashboard.", namespace);
    if (name != null) {
      rowKeyString += name;
    }
    return rowKeyString;
  }
}
