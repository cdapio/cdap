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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.hbase.MetricHBaseTableUtil;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.process.KafkaConsumerMetaTable;
import co.cask.cdap.metrics.store.timeseries.EntityTable;
import co.cask.cdap.metrics.store.timeseries.FactTable;
import co.cask.cdap.metrics.store.upgrade.MetricsDataMigrator;
import co.cask.cdap.proto.Id;
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
    this(new NamespacedDatasetFramework(dsFramework, new DefaultDatasetNamespace(cConf)), cConf);
  }

  private DefaultMetricDatasetFactory(DatasetFramework namespacedDsFramework, final CConfiguration cConf) {
    this.cConf = cConf;
    this.dsFramework = namespacedDsFramework;

    this.entityTable = Suppliers.memoize(new Supplier<EntityTable>() {

      @Override
      public EntityTable get() {
        String tableName = cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                     MetricsConstants.DEFAULT_ENTITY_TABLE_NAME);
        return new EntityTable(getOrCreateMetricsTable(tableName, DatasetProperties.EMPTY));
      }
    });
  }

  // todo: figure out roll time based on resolution from config? See DefaultMetricsTableFactory for example
  @Override
  public FactTable get(int resolution) {
    String tableName = cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                 MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX) + ".ts." + resolution;
    int ttl =  cConf.getInt(MetricsConstants.ConfigKeys.RETENTION_SECONDS + "." + resolution + ".seconds", -1);

    DatasetProperties.Builder props = DatasetProperties.builder();
    // don't add TTL for MAX_RESOLUTION table. CDAP-1626
    if (ttl > 0 && resolution != Integer.MAX_VALUE) {
      props.add(Table.PROPERTY_TTL, ttl);
    }
    // for efficient counters
    props.add(Table.PROPERTY_READLESS_INCREMENT, "true");

    MetricsTable table = getOrCreateMetricsTable(tableName, props.build());
    LOG.info("FactTable created: {}", tableName);
    return new FactTable(table, entityTable.get(), resolution, getRollTime(resolution));
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

  private MetricsTable getOrCreateMetricsTable(String tableName, DatasetProperties props) {

    MetricsTable table = null;
    // metrics tables are in the system namespace
    Id.DatasetInstance metricsDatasetInstanceId = Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE, tableName);
    while (table == null) {
      try {
        table = DatasetsUtil.getOrCreateDataset(dsFramework, metricsDatasetInstanceId,
                                                MetricsTable.class.getName(), props, null, null);
      } catch (DatasetManagementException e) {
        // dataset service may be not up yet
        // todo: seems like this logic applies everywhere, so should we move it to DatasetsUtil?
        LOG.warn("Cannot access or create table {}, will retry in 1 sec.", tableName);
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      } catch (IOException e) {
        LOG.error("Exception while creating table {}.", tableName, e);
        throw Throwables.propagate(e);
      }
    }

    return table;
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by metrics system.
   * <p/>
   * It is primarily used by upgrade tool.
   *
   * @param datasetFramework framework to add types and datasets to
   */
  public static MetricDatasetFactory setupDatasets(CConfiguration conf, DatasetFramework datasetFramework)
    throws IOException, DatasetManagementException {

    DefaultMetricDatasetFactory factory = new DefaultMetricDatasetFactory(datasetFramework, conf);

    // adding all fact tables
    factory.get(1);
    factory.get(60);
    factory.get(3600);
    factory.get(Integer.MAX_VALUE);

    // adding kafka consumer meta
    factory.createKafkaConsumerMeta();
    return factory;
  }

  /**
   * Migrates metrics data from version 2.7 and older to 2.8
   * @param conf configuration
   * @param datasetFramework framework to add types and datasets to
   * @param version - version we are migrating the data from
   * @throws IOException
   * @throws DatasetManagementException
   */
  public static void migrateData(CConfiguration conf, DatasetFramework datasetFramework,
                                 MetricHBaseTableUtil.Version version)
    throws IOException, DatasetManagementException {
    MetricsDataMigrator migrator = new MetricsDataMigrator(conf, datasetFramework,
                                                           setupDatasets(conf, datasetFramework));
    migrator.migrateMetricsTables(version);
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
