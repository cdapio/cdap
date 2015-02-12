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

package co.cask.cdap.metrics.store;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.data.EntityTable;
import co.cask.cdap.metrics.process.KafkaConsumerMetaTable;
import co.cask.cdap.metrics.store.timeseries.FactTable;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DefaultMetricDatasetFactory implements MetricDatasetFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricDatasetFactory.class);

  private final CConfiguration cConf;
  private final Supplier<EntityTable> entityTable;
  private final DatasetFramework dsFramework;

  @Inject
  public DefaultMetricDatasetFactory(final CConfiguration cConf, final DatasetFramework dsFramework) {
    this.cConf = cConf;
    this.dsFramework =
      new NamespacedDatasetFramework(dsFramework,
                                     new DefaultDatasetNamespace(cConf, Namespace.SYSTEM));

    this.entityTable = Suppliers.memoize(new Supplier<EntityTable>() {

      @Override
      public EntityTable get() {
        String tableName = cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                     // todo: remove + ".v2"
                                     MetricsConstants.DEFAULT_ENTITY_TABLE_NAME) + ".v2";
        MetricsTable table = null;
        while (table == null) {
          try {
            table = getOrCreateMetricsTable(tableName, DatasetProperties.EMPTY);
          } catch (DatasetManagementException e) {
            // dataset service may be not up yet
            // todo: seems like this logic applies everywhere, so should we move it to DatasetsUtil?
            LOG.warn("Cannot access entityTable, will retry in 1 sec.");
            try {
              TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              break;
            }
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }
        return new EntityTable(table);
      }
    });
  }

  // todo: figure out roll time based on resolution from config? See DefaultMetricsTableFactory for example
  @Override
  public FactTable get(int resolution) {
    try {
      String tableName = cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                   // todo: remove + ".v2"
                                   MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX) + ".v2" + ".ts." + resolution;
      int ttl =  cConf.getInt(MetricsConstants.ConfigKeys.RETENTION_SECONDS + "." + resolution + ".seconds", -1);

      DatasetProperties props = ttl > 0 ?
        DatasetProperties.builder().add(OrderedTable.PROPERTY_TTL, ttl).build() : DatasetProperties.EMPTY;
      MetricsTable table = getOrCreateMetricsTable(tableName, props);
      LOG.info("FactTable created: {}", tableName);
      return new FactTable(table, entityTable.get(), resolution, getRollTime(resolution));
    } catch (Exception e) {
      LOG.error("Exception in creating FactTable.", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public KafkaConsumerMetaTable createKafkaConsumerMeta() {
    try {
      String tableName = cConf.get(MetricsConstants.ConfigKeys.KAFKA_META_TABLE,
                                   MetricsConstants.DEFAULT_KAFKA_META_TABLE);
      MetricsTable table = getOrCreateMetricsTable(tableName, DatasetProperties.EMPTY);
      LOG.info("KafkaConsumerMetaTable created: {}", tableName);
      return new KafkaConsumerMetaTable(table);
    } catch (Exception e) {
      LOG.error("Exception in creating KafkaConsumerMetaTable.", e);
      throw Throwables.propagate(e);
    }
  }

  private MetricsTable getOrCreateMetricsTable(String tableName, DatasetProperties props)
    throws DatasetManagementException, IOException {

    return DatasetsUtil.getOrCreateDataset(dsFramework, tableName, MetricsTable.class.getName(), props, null, null);
  }

  @Override
  public void upgrade() throws Exception {
    // todo: remove + ".v2"
    String metricsPrefix = cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                     MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX) + ".v2";
    for (DatasetSpecification spec : dsFramework.getInstances()) {
      String dsName = spec.getName();
      if (dsName.contains(metricsPrefix + ".ts.")) {
        DatasetAdmin admin = dsFramework.getAdmin(dsName, null);
        if (admin != null) {
          admin.upgrade();
        } else {
          LOG.error("Could not obtain admin to upgrade metrics table: " + dsName);
          // continue to best effort
        }
      }
    }
  }

  private int getRollTime(int resolution) {
    String key = MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME + "." + resolution;
    String value = cConf.get(key);
    if (value != null) {
      return cConf.getInt(key, MetricsConstants.DEFAULT_TIME_SERIES_TABLE_ROLL_TIME);
    }
    return cConf.getInt(MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME,
                        MetricsConstants.DEFAULT_TIME_SERIES_TABLE_ROLL_TIME);
  }

}
