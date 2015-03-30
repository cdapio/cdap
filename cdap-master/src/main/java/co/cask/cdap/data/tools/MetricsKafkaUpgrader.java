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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTableAdmin;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.process.KafkaConsumerMetaTable;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.proto.Id;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.kafka.client.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Upgrade kafka metrics meta table data
 */
public class MetricsKafkaUpgrader extends AbstractUpgrader {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetUpgrader.class);
  private static final byte[] OFFSET_COLUMN = Bytes.toBytes("o");

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final HBaseTableUtil hBaseTableUtil;
  private final DatasetFramework dsFramework;
  private final String oldKafkaMetricsTableName;

  @Inject
  public MetricsKafkaUpgrader(CConfiguration cConf, Configuration hConf, LocationFactory locationFactory,
                              NamespacedLocationFactory namespacedLocationFactory, HBaseTableUtil hBaseTableUtil,
                              final DatasetFramework dsFramework) {
    super(locationFactory, namespacedLocationFactory);
    this.cConf = cConf;
    this.hConf = hConf;
    this.hBaseTableUtil = hBaseTableUtil;
    this.dsFramework = dsFramework;
    this.oldKafkaMetricsTableName =  Joiner.on(".").join(Constants.SYSTEM_NAMESPACE, "default",
                                                         cConf.get(MetricsConstants.ConfigKeys.KAFKA_META_TABLE,
                                                                   MetricsConstants.DEFAULT_KAFKA_META_TABLE));
  }

  private MetricsTable getOrCreateKafkaTable(String tableName) {
    // old kafka table is in the default namespace
    Id.DatasetInstance metricsDatasetInstanceId = Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE_ID, tableName);
    try {
      return DatasetsUtil.getOrCreateDataset(dsFramework, metricsDatasetInstanceId,
                                              MetricsTable.class.getName(), DatasetProperties.EMPTY, null, null);
    } catch (Exception e) {
      LOG.error("Exception while getting table {}.", tableName, e);
      throw Throwables.propagate(e);
    }
  }

  public boolean tableExists() throws Exception {
    TableId tableId = getOldKafkaMetricsTableId();
    if (!hBaseTableUtil.tableExists(new HBaseAdmin(hConf), tableId)) {
      LOG.info("Table does not exist: {}. No upgrade necessary.", tableId);
      return false;
    }
    return true;
  }

  @Override
  public void upgrade() throws Exception {
    // todo : close the KafkaConsumerMetaTable after it implements closeable
    KafkaConsumerMetaTable kafkaMetaTableDestination =
      new DefaultMetricDatasetFactory(cConf, dsFramework).createKafkaConsumerMeta();
    // copy kafka offset from old table to new kafka metrics table
    try {
      // assuming we are migrating from 2.6
      HTable hTable = getHTable(oldKafkaMetricsTableName);
      // iterate old table and copy all rows to the new kafkaConsumerMetaTable
      try {
        byte[] columnFamily = getColumnFamily(oldKafkaMetricsTableName);
        LOG.info("Starting upgrade for table {}", Bytes.toString(hTable.getTableName()));
        Scan scan = getScan(columnFamily);
        ResultScanner resultScanner = hTable.getScanner(scan);
        try {
          Result result;
          while ((result = resultScanner.next()) != null) {
            TopicPartition topicPartition = getTopicPartition(result.getRow());
            if (topicPartition != null) {
              long value  = Bytes.toLong(result.getFamilyMap(columnFamily).get(OFFSET_COLUMN));
              kafkaMetaTableDestination.save(ImmutableMap.of(topicPartition, value));
            } else {
              LOG.warn("Invalid topic partition found {}", Bytes.toStringBinary(result.getRow()));
            }
          }
          LOG.info("Successfully completed upgrade for table {}", Bytes.toString(hTable.getTableName()));
        } finally {
          resultScanner.close();
        }
      } catch (Exception e) {
        LOG.info("Exception during upgrading metrics-kafka table {}", e);
        throw Throwables.propagate(e);
      } finally {
        hTable.close();
      }
    } catch (IOException e) {
      LOG.info("Unable to find table {}", oldKafkaMetricsTableName);
    }
  }

  private byte[] getColumnFamily(String tableName) throws DatasetManagementException, IOException {
    // add old table for getting dataset spec
    MetricsTable table = getOrCreateKafkaTable(tableName);
    try {
      DatasetSpecification specification =
        dsFramework.getDatasetSpec(Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE_ID, tableName));
      return HBaseTableAdmin.getColumnFamily(specification);
    } finally {
      table.close();
    }
  }

  private HTable getHTable(String tableName) throws IOException {
    TableId kafkaTableOld = TableId.from(Constants.DEFAULT_NAMESPACE, tableName);
    return hBaseTableUtil.createHTable(hConf, kafkaTableOld);
  }

  private Scan getScan(byte[] columnFamily) throws IOException {
    Scan scan = new Scan();
    scan.setTimeRange(0, HConstants.LATEST_TIMESTAMP);
    scan.addFamily(columnFamily);
    scan.setMaxVersions(1); // we only need to see one version of each row
    return scan;
  }

  private TopicPartition getTopicPartition(byte[] rowKey) {
    // convert rowkey to string, split by "DOT" , create TopicPartition
    List<String> tp = Arrays.asList(Bytes.toString(rowKey).split("\\."));
    if (tp.size() > 1) {
      int length = tp.size();
      String topic = Joiner.on(".").join(tp.subList(0, length - 1));
      int partition = Integer.parseInt(tp.get(length - 1));
      return new TopicPartition(topic, partition);
    }
    return null;
  }

  public TableId getOldKafkaMetricsTableId() {
    return TableId.from(Constants.DEFAULT_NAMESPACE, oldKafkaMetricsTableName);
  }
}
