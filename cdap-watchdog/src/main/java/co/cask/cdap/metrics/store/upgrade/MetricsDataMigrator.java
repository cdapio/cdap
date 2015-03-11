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
package co.cask.cdap.metrics.store.upgrade;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.DatasetNamespace;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.hbase.MetricHBaseTableUtil;
import co.cask.cdap.data2.dataset2.lib.table.hbase.MetricHBaseTableUtil.Version;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.metrics.store.timeseries.EntityTable;
import co.cask.cdap.proto.Id;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Migration for metrics data from 2.6 to 2.8
 */
public class MetricsDataMigrator {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsDataMigrator.class);
  private static final String TYPE = "type";
  private static final List<String> scopes = ImmutableList.of("system", "user");
  private static final Map<String, List<String>> typeToTagNameMapping =
    ImmutableMap.<String, List<String>>builder()
      .put("f", ImmutableList.of(Constants.Metrics.Tag.APP,
                                 TYPE,
                                 Constants.Metrics.Tag.FLOW,
                                 Constants.Metrics.Tag.FLOWLET,
                                 Constants.Metrics.Tag.INSTANCE_ID))
      .put("b", ImmutableList.of(Constants.Metrics.Tag.APP,
                                 TYPE,
                                 Constants.Metrics.Tag.MAPREDUCE,
                                 Constants.Metrics.Tag.MR_TASK_TYPE,
                                 Constants.Metrics.Tag.INSTANCE_ID))
      .put("p", ImmutableList.of(Constants.Metrics.Tag.APP,
                                 TYPE,
                                 Constants.Metrics.Tag.PROCEDURE,
                                 Constants.Metrics.Tag.INSTANCE_ID))

      .put("s", ImmutableList.of(Constants.Metrics.Tag.APP,
                                 TYPE,
                                 Constants.Metrics.Tag.SPARK,
                                 Constants.Metrics.Tag.INSTANCE_ID))
      .put("u", ImmutableList.of(Constants.Metrics.Tag.APP,
                                 TYPE,
                                 Constants.Metrics.Tag.SERVICE,
                                 Constants.Metrics.Tag.SERVICE_RUNNABLE,
                                 Constants.Metrics.Tag.INSTANCE_ID))
      .put("w", ImmutableList.of(Constants.Metrics.Tag.APP,
                                 TYPE,
                                 Constants.Metrics.Tag.WORKFLOW,
                                 Constants.Metrics.Tag.INSTANCE_ID))
      .build();

  private static final Map<String, String> metricNameToTagNameMapping =
    ImmutableMap.<String, String>builder()
      .put("store.reads", Constants.Metrics.Tag.DATASET)
      .put("store.writes", Constants.Metrics.Tag.DATASET)
      .put("store.ops", Constants.Metrics.Tag.DATASET)
      .put("store.bytes", Constants.Metrics.Tag.DATASET)
      .put("dataset.store.reads", Constants.Metrics.Tag.DATASET)
      .put("dataset.store.writes", Constants.Metrics.Tag.DATASET)
      .put("dataset.store.ops", Constants.Metrics.Tag.DATASET)
      .put("dataset.store.bytes", Constants.Metrics.Tag.DATASET)
      .put("dataset.size.mb", Constants.Metrics.Tag.DATASET)
      .put("collect.events", Constants.Metrics.Tag.STREAM)
      .put("collect.bytes", Constants.Metrics.Tag.STREAM)
      .put("process.tuples.read", Constants.Metrics.Tag.FLOWLET_QUEUE)
      .put("process.events.in", Constants.Metrics.Tag.FLOWLET_QUEUE)
      .put("process.events.out", Constants.Metrics.Tag.FLOWLET_QUEUE)
      .put("process.events.processed", Constants.Metrics.Tag.FLOWLET_QUEUE)
      .build();

  private static final Map<String, Map<String, String>> mapOldSystemContextToNew =
    ImmutableMap.<String, Map<String, String>>of(
      "transactions", ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE,
                                      Constants.Metrics.Tag.COMPONENT, "transactions"),
      "-", ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE),

      "gateway", ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE,
                                 Constants.Metrics.Tag.COMPONENT, Constants.Gateway.METRICS_CONTEXT,
                                 Constants.Metrics.Tag.HANDLER, Constants.Gateway.STREAM_HANDLER_NAME)
    );

  private final DatasetFramework dsFramework;
  private final MetricStore aggMetricStore;
  private final String entityTableName;
  private final String metricsTableNamePrefix;
  private final String metricsTableName;
  private final CConfiguration cConf;
  private final Configuration hConf;


  public MetricsDataMigrator(final CConfiguration cConf, final Configuration hConf, final DatasetFramework dsFramework,
                             MetricDatasetFactory factory) {
    this.dsFramework = dsFramework;
    this.entityTableName = cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                     UpgradeMetricsConstants.DEFAULT_ENTITY_TABLE_NAME);
    this.metricsTableNamePrefix = cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                            UpgradeMetricsConstants.DEFAULT_METRICS_TABLE_PREFIX);
    this.metricsTableName = metricsTableNamePrefix + ".agg";
    aggMetricStore = new DefaultMetricStore(factory, new int[]{Integer.MAX_VALUE});
    this.cConf = cConf;
    this.hConf = hConf;
  }

  public void migrateMetricsTables(HBaseTableUtil hBaseTableUtil,
                                   boolean keepOldData) throws DataMigrationException {
    Version cdapVersion = findMetricsTableVersion(new MetricHBaseTableUtil(hBaseTableUtil));
    if (cdapVersion == Version.VERSION_2_6_OR_LOWER) {
      migrateMetricsTableFromVersion26(cdapVersion);
    } else if (cdapVersion == Version.VERSION_2_7) {
      migrateMetricsTableFromVersion27(cdapVersion);
    } else {
      System.out.println("Unsupported version" + cdapVersion);
      return;
    }
    if (!keepOldData) {
      System.out.println("Performing cleanup of old metrics tables");
      cleanUpOldTables(cdapVersion);
    }
  }

  @Nullable
  private Version findMetricsTableVersion(MetricHBaseTableUtil metricHBaseTableUtil) {

    // Figure out what is the latest working version of CDAP
    // 1) if latest is 2.8.x - nothing to do, if "pre-2.8", proceed to next step.
    // 2) find a most recent metrics table: start by looking for 2.7, then 2.6
    // 3) if we find 2.7 - we will migrate data from 2.7 table, if not - migrate data from 2.6 metrics table
    // todo - use UpgradeTool to figure out if version is 2.8.x, return if it is 2.8.x

    String tableName27 = cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                   UpgradeMetricsConstants.DEFAULT_METRICS_TABLE_PREFIX) + ".agg";

    // versions older than 2.7, has two metrics table, identified by system and user prefix
    String tableName26 = "system." + tableName27;
    DefaultDatasetNamespace defaultDatasetNamespace = new DefaultDatasetNamespace(cConf);
    String metricsEntityTable26 = defaultDatasetNamespace.namespace(new Id.Namespace(Constants.SYSTEM_NAMESPACE),
                                                                    tableName26);
    String metricsEntityTable27 = defaultDatasetNamespace.namespace(new Id.Namespace(Constants.SYSTEM_NAMESPACE),
                                                                    tableName27);

    Version version = null;
    try {
      HBaseAdmin  hAdmin = new HBaseAdmin(hConf);
      for (HTableDescriptor desc : hAdmin.listTables()) {
        if (desc.getNameAsString().equals(metricsEntityTable27)) {
          System.out.println("Matched HBase Table Name For Migration " + desc.getNameAsString());
          version = metricHBaseTableUtil.getVersion(desc);
          version = verifyVersion(Version.VERSION_2_7, version);
          if (version == null) {
            return null;
          }
          break;
        }
        if (desc.getNameAsString().equals(metricsEntityTable26)) {
          System.out.println("Matched HBase Table Name For Migration " + desc.getNameAsString());
          version = metricHBaseTableUtil.getVersion(desc);
          version = verifyVersion(Version.VERSION_2_6_OR_LOWER, version);
          if (version == null) {
            return null;
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return version;
  }

  private Version verifyVersion(Version expected, Version actual) {
    if (expected != actual) {
      System.out.println("Version detected based on table name does not match table configuration");
      return null;
    }
    return actual;
  }

  private void migrateMetricsTableFromVersion26(Version version) throws DataMigrationException {
    for (String scope : scopes) {
      EntityTable entityTable = new EntityTable(getOrCreateMetricsTable(String.format("%s.%s", scope, entityTableName),
                                                                        DatasetProperties.EMPTY));
      String scopedMetricsTableName = String.format("%s.%s", scope, metricsTableName);
      MetricsTable metricsTable = getOrCreateMetricsTable(scopedMetricsTableName, DatasetProperties.EMPTY);
      System.out.println("Migrating Metrics Data from table : " + scopedMetricsTableName);
      migrateMetricsData(entityTable, metricsTable, scope, version);
    }
  }

  public void cleanUpOldTables(Version version) throws DataMigrationException {
    Set<String> tablesToDelete = Sets.newHashSet();
    DefaultDatasetNamespace defaultDatasetNamespace = new DefaultDatasetNamespace(cConf);

    // add kafka meta table to deleteList
    tablesToDelete.add(addNamespace(defaultDatasetNamespace, cConf.get(MetricsConstants.ConfigKeys.KAFKA_META_TABLE,
                                                                       MetricsConstants.DEFAULT_KAFKA_META_TABLE)));

    if (version == Version.VERSION_2_6_OR_LOWER) {
      List<String> scopes = ImmutableList.of("system", "user");
      // add user and system - entity tables , aggregates table and time series table to the list
      for (String scope : scopes) {
        addTableNamesToDelete(tablesToDelete, cConf, scope, ImmutableList.of(1));
      }
    }

    if (version == Version.VERSION_2_7) {
      addTableNamesToDelete(tablesToDelete, cConf, null, ImmutableList.of(1, 60, 3600));
    }

    System.out.println("Deleting Tables : " + tablesToDelete);
    deleteTables(hConf, tablesToDelete);
  }

  private void deleteTables(Configuration hConf, Set<String> tablesToDelete) throws DataMigrationException {
    HBaseAdmin hAdmin = null;
    try {
      hAdmin = new HBaseAdmin(hConf);
      for (HTableDescriptor desc : hAdmin.listTables()) {
        if (tablesToDelete.contains(desc.getNameAsString())) {
          // disable the table
          hAdmin.disableTable(desc.getName());
          // delete the table
          hAdmin.deleteTable(desc.getName());
        }
      }
    } catch (Exception e) {
      LOG.error("Exception while trying to delete old metrics tables", e);
      throw new DataMigrationException("Failed deleting old metrics tables");
    }
  }

  private String addNamespace(DatasetNamespace dsNamespace, String tableName) {
    return addNamespace(dsNamespace, null , tableName);
  }
  private String addNamespace(DatasetNamespace dsNamespace, String scope, String tableName) {
    tableName = scope == null ? tableName : scope + "." + tableName;
    return dsNamespace.namespace(new Id.Namespace(Constants.SYSTEM_NAMESPACE), tableName);
  }
  private void addTableNamesToDelete(Set<String> tablesToDelete, CConfiguration cConf,
                                     String scope, List<Integer> resolutions) {
    DefaultDatasetNamespace defaultDatasetNamespace = new DefaultDatasetNamespace(cConf);
    tablesToDelete.add(addNamespace(defaultDatasetNamespace, scope, entityTableName));
    // add aggregates table
    tablesToDelete.add(addNamespace(defaultDatasetNamespace, scope, metricsTableName));

    // add timeseries tables
    for (int resolution : resolutions) {
      tablesToDelete.add(addNamespace(defaultDatasetNamespace, scope, metricsTableNamePrefix + ".ts." + resolution));
    }
  }

  private void migrateMetricsTableFromVersion27(Version version) throws DataMigrationException {
    EntityTable entityTable = new EntityTable(getOrCreateMetricsTable(entityTableName, DatasetProperties.EMPTY));
    MetricsTable metricsTable = getOrCreateMetricsTable(metricsTableName, DatasetProperties.EMPTY);
    System.out.println("Migrating Metrics Data from table : " + metricsTableName);
    migrateMetricsData(entityTable, metricsTable, null, version);
  }

  private void migrateMetricsData(EntityTable entityTable, MetricsTable metricsTable, String scope,
                                  Version version) {
    MetricsEntityCodec codec = getEntityCodec(entityTable);
    int idSize = getIdSize(version);
    Row row;
    long rowCount = 0;
    try {
      Scanner scanner = metricsTable.scan(null, null, null, null);
      while ((row = scanner.next()) != null) {
        byte[] rowKey = row.getRow();
        int offset = 0;
        String context = codec.decode(MetricsEntityType.CONTEXT, rowKey, offset, idSize);
        context = getContextBasedOnVersion(context, version);
        offset += codec.getEncodedSize(MetricsEntityType.CONTEXT, idSize);
        String metricName = codec.decode(MetricsEntityType.METRIC, rowKey, offset, idSize);
        offset += codec.getEncodedSize(MetricsEntityType.METRIC, idSize);
        scope = getScopeBasedOnVersion(scope, metricName, version);
        metricName = getMetricNameBasedOnVersion(metricName, version);
        String runId = codec.decode(MetricsEntityType.RUN, rowKey, offset, idSize);
        parseAndAddNewMetricValue(scope, context, metricName, runId, row.getColumns());
        rowCount++;
        printStatus(rowCount);
      }
      System.out.println("Migrated " + rowCount + " records");
   } catch (Exception e) {
      LOG.warn("Exception during data-transfer in aggregates table", e);
      //no-op
   }
  }

  private void printStatus(long rowCount) {
    if (rowCount % 10000 == 0) {
      System.out.println("Migrated " + rowCount + " records.");
    }
  }

  private MetricsEntityCodec getEntityCodec(EntityTable table) {
    return new MetricsEntityCodec(table, UpgradeMetricsConstants.DEFAULT_CONTEXT_DEPTH,
                                  UpgradeMetricsConstants.DEFAULT_METRIC_DEPTH,
                                  UpgradeMetricsConstants.DEFAULT_TAG_DEPTH);
  }

  private void addMetrics(Map<byte[], byte[]> columns, String metricName,
                          Map<String, String> tagMap, String metricTagType) throws Exception {
    for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
      String tagValue = Bytes.toString(entry.getKey());
      if (metricTagType != null) {
        if (tagValue.equals(UpgradeMetricsConstants.EMPTY_TAG)) {
          continue;
        } else {
          long value = Bytes.toLong(entry.getValue());
          Map<String, String> tagValues = Maps.newHashMap(tagMap);
          tagValues.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
          tagValues.put(metricTagType, tagValue);
          addMetricValueToMetricStore(tagValues, metricName, 0, value, MetricType.COUNTER);
        }
      } else {
        addMetricValueToMetricStore(tagMap, metricName, 0, Bytes.toLong(entry.getValue()),
                                    MetricType.COUNTER);
      }
    }
  }

  private void parseAndAddNewMetricValue(String scope, String context, String metricName, String runId,
                                         Map<byte[], byte[]> columns) throws Exception {

    List<String> contextParts =  Lists.newArrayList(Splitter.on(".").split(context));
    Map<String, String> tagMap = Maps.newHashMap();
    tagMap.put(Constants.Metrics.Tag.SCOPE, scope);
    if (runId != null) {
      tagMap.put(Constants.Metrics.Tag.RUN_ID, runId);
    }
    Map<String, String> systemMap = null;
    if (contextParts.size() > 0) {
      systemMap = mapOldSystemContextToNew.get(contextParts.get(0));
    }
    String tagKey = metricNameToTagNameMapping.get(metricName);
    if (systemMap != null) {
      tagMap.putAll(systemMap);
    } else  if (contextParts.size() > 1) {
      // application metrics
      List<String> targetTagList = typeToTagNameMapping.get(contextParts.get(1));
      if (targetTagList != null) {
        populateApplicationTags(tagMap, contextParts, targetTagList, context);
      } else {
        // service metrics , we are skipping them as they were not exposed for querying before
        LOG.trace("Skipping context : {}", context);
        return;
      }
    } else {
      System.out.println(String.format("Unexpected metric context %s.", context));
    }
    LOG.trace("Adding metrics - tagMap : {} - context : {} - metricName : {} and tagKey : {}", tagMap, context,
              metricName, tagKey);
    addMetrics(columns, metricName, tagMap, tagKey);
  }

  private void populateApplicationTags(Map<String, String> tagMap, List<String> contextParts,
                                       List<String> targetTagList, String context) {
    tagMap.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
    for (int i = 0; i < contextParts.size(); i++) {
      if (i == targetTagList.size()) {
        LOG.trace(" Context longer than targetTagList" + context);
        break;
      }
      if (targetTagList.get(i).equals(TYPE)) {
        continue;
      }
      tagMap.put(targetTagList.get(i), contextParts.get(i));
    }
  }

  // constructs MetricValue based on parameters passed and adds the MetricValue to MetricStore.
  private void addMetricValueToMetricStore(Map<String, String> tags, String metricName,
                                           int timeStamp, long value, MetricType counter) throws Exception {
    aggMetricStore.add(new MetricValue(tags, metricName, timeStamp, value, counter));
  }

  private MetricsTable getOrCreateMetricsTable(String tableName, DatasetProperties empty)
    throws DataMigrationException {
    MetricsTable table = null;
    // for default namespace, we have to provide the complete table name.
    tableName = "system." + tableName;
    // metrics tables are in the system namespace
    Id.DatasetInstance metricsDatasetInstanceId = Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE, tableName);
    try {
      table = DatasetsUtil.getOrCreateDataset(dsFramework, metricsDatasetInstanceId,
                                              MetricsTable.class.getName(), empty, null, null);
    } catch (DatasetManagementException e) {
      String msg = String.format("Cannot access or create table %s.", tableName);
      LOG.error(msg);
      throw new DataMigrationException(msg);
    } catch (IOException e) {
      String msg = String.format("Exception while creating table %s", tableName);
      LOG.error(msg, e);
      throw new DataMigrationException(msg);
    }
    return table;
  }

  public void cleanupDestinationTables() throws DataMigrationException {
    System.out.println("Cleaning up destination tables");
    String rootPrefix = cConf.get(Constants.Dataset.TABLE_PREFIX) + "_";
    String destEntityTableName =  cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                            MetricsConstants.DEFAULT_ENTITY_TABLE_NAME);
    destEntityTableName = getTableName(rootPrefix, Id.DatasetInstance.from(
      Id.Namespace.from(Constants.SYSTEM_NAMESPACE), destEntityTableName));
    String destMetricsTablePrefix =  cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                               MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX);
    destMetricsTablePrefix = getTableName(rootPrefix, Id.DatasetInstance.from(
      Id.Namespace.from(Constants.SYSTEM_NAMESPACE), destMetricsTablePrefix));
    try {
      HBaseAdmin hAdmin = new HBaseAdmin(hConf);
      for (HTableDescriptor desc : hAdmin.listTables()) {
        if (desc.getNameAsString().equals(destEntityTableName) ||
          desc.getNameAsString().startsWith(destMetricsTablePrefix)) {
          System.out.println(String.format("Deleting table %s before upgrade", desc.getNameAsString()));
          //disable the table
          hAdmin.disableTable(desc.getName());
          //delete the table
          hAdmin.deleteTable(desc.getName());
        }
      }
    } catch (Exception e) {
      LOG.error("Exception during cleanup of destination tables " + e);
      throw new DataMigrationException("Failed Cleaning up destination tables");
    }
  }

  private String getTableName(String rootPrefix, Id.DatasetInstance instance) {
    return  rootPrefix + instance.getNamespaceId() + ":" + instance.getId();
  }

  private String getMetricNameBasedOnVersion(String metricName, Version version) {
    if (version == Version.VERSION_2_6_OR_LOWER) {
      return metricName;
    } else {
      // metric name has scope prefix, lets remove the scope prefix and return
      return metricName.substring(metricName.indexOf(".") + 1);
    }
  }

  private int getIdSize(Version version) {
    if (version == Version.VERSION_2_6_OR_LOWER) {
      // we use 2 bytes in 2.6
      return 2;
    } else {
      // we increased the size to 3 bytes from 2.7
      return 3;
    }
  }
  private String getContextBasedOnVersion(String context, Version version) {
    if (version == Version.VERSION_2_6_OR_LOWER) {
      return context;
    } else {
      // skip namespace - some metrics are emitted in system namespace though they have app-name, dataset name, etc
      // so lets skip and figure out manually
      return context.substring(context.indexOf(".") + 1);
    }
  }

  private String getScopeBasedOnVersion(String scope, String metricName, Version version) {
    if (version == Version.VERSION_2_6_OR_LOWER) {
      return scope;
    } else {
      // metric name has scope prefix, lets split that
      return metricName.substring(0, metricName.indexOf("."));
    }
  }

}
