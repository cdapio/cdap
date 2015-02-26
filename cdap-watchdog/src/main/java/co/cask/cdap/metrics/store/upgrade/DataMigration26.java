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
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.metrics.data.EntityTable;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.proto.Id;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Migration for metrics data from 2.6 to 2.8
 */
public class DataMigration26 {

  private static final Logger LOG = LoggerFactory.getLogger(DataMigration26.class);
  private static final String ENTITY_TABLE_NAME_SUFFIX = ".metrics.entity";
  private static final String AGGREGATES_TABLE_NAME_SUFFIX = ".metrics.table.agg";
  private static final String TYPE = "type";

  private final DatasetFramework dsFramework;
  private final MetricStore aggMetricStore;
  private final List<String> scopes = ImmutableList.of("system", "user");
  private final Map<String, List<String>> typeToTagNameMapping =
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

  private final Map<String, String> metricNameToTagNameMapping =
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

  private final Map<String, Map<String, String>> mapOldSystemContextToNew =
    ImmutableMap.<String, Map<String, String>>of(
      "transactions", ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE,
                                      Constants.Metrics.Tag.COMPONENT, "transactions"),
      "-", ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE),

      "gateway", ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE,
                                 Constants.Metrics.Tag.COMPONENT, Constants.Gateway.METRICS_CONTEXT,
                                 Constants.Metrics.Tag.HANDLER, Constants.Gateway.STREAM_HANDLER_NAME)
    );

  private Map<String, MetricsTable> scopeToAggregatesTable = Maps.newHashMap();
  private Map<String, EntityTable> scopeToEntityTable = Maps.newHashMap();
  private Map<String, MetricsEntityCodec> scopeToCodec = Maps.newHashMap();

  public DataMigration26(final CConfiguration cConf, final DatasetFramework dsFramework,
                         DefaultMetricDatasetFactory factory) {

    //todo - change hard-coded table name to read from configuration instead.
    LOG.info("Initializing Data Migration.");
    this.dsFramework = dsFramework;
    for (String scope : scopes) {
      scopeToEntityTable.put(scope, new EntityTable(getOrCreateMetricsTable(scope + ENTITY_TABLE_NAME_SUFFIX,
                                                                            DatasetProperties.EMPTY)));
      scopeToAggregatesTable.put(scope, getOrCreateMetricsTable(scope + AGGREGATES_TABLE_NAME_SUFFIX,
                                                                DatasetProperties.EMPTY));
      scopeToCodec.put(scope, new MetricsEntityCodec(scopeToEntityTable.get(scope),
                                                     UpgradeMetricsConstants.DEFAULT_CONTEXT_DEPTH,
                                                     UpgradeMetricsConstants.DEFAULT_METRIC_DEPTH,
                                                     UpgradeMetricsConstants.DEFAULT_TAG_DEPTH));
    }
    aggMetricStore = new DefaultMetricStore(factory, new int[]{Integer.MAX_VALUE});
  }

  public void decodeAggregatesTable26() {
    try {
      for (String scope : scopes) {
        Scanner scanner = scopeToAggregatesTable.get(scope).scan(null, null, null, null);
        MetricsEntityCodec codec = scopeToCodec.get(scope);
        LOG.info("Decoding for scope " + scope + "on table " + scopeToAggregatesTable.get(scope).toString() +
                   " using codec " + codec.toString());
        Row row;
        while ((row = scanner.next()) != null) {
          byte[] rowKey = row.getRow();
          int offset = 0;
          String context = codec.decode26(MetricsEntityType.CONTEXT, rowKey, offset);
          offset += codec.getEncodedSize26(MetricsEntityType.CONTEXT);
          String metricName = codec.decode26(MetricsEntityType.METRIC, rowKey, offset);
          offset += codec.getEncodedSize26(MetricsEntityType.METRIC);
          String runId = codec.decode26(MetricsEntityType.RUN, rowKey, offset);
          constructMetricValue(scope, context, metricName, runId, row.getColumns().entrySet().iterator());
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception during data-transfer in aggregates table", e);
      // no-op
    }
  }

  private void emitMetrics(Iterator<Map.Entry<byte[], byte[]>> iterator, String context, String metricName,
                           Map<String, String> tagMap, String nonEmptyTagKey) throws Exception {
    while (iterator.hasNext()) {
      Map.Entry<byte[], byte[]> entry = iterator.next();
      String tagValue = Bytes.toString(entry.getKey());
      if (nonEmptyTagKey != null) {
        if (tagValue.equals(UpgradeMetricsConstants.EMPTY_TAG)) {
          continue;
        } else {
          long value = Bytes.toLong(entry.getValue());
          Map<String, String> newMap = Maps.newHashMap(tagMap);
          newMap.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
          newMap.put(nonEmptyTagKey, tagValue);
          sendMetrics(aggMetricStore, newMap, metricName, 0, value, MetricType.GAUGE);
          LOG.info("Sending Metric for context {} with tagKey {} and tagValue {}", context, nonEmptyTagKey, tagValue);
        }
      } else {
        LOG.info("Context without tagKey {}", context);
        sendMetrics(aggMetricStore, tagMap, metricName, 0, Bytes.toLong(entry.getValue()), MetricType.GAUGE);
      }
    }
  }

  private void constructMetricValue(String scope, String context, String metricName, String runId,
                                    Iterator<Map.Entry<byte[], byte[]>> iterator) throws Exception {

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
        // todo : remove logging
        LOG.info("Skipping context : {}", context);
        return;
      }
    } else {
      LOG.info("Should not reach here {}", context);
    }
    LOG.info("Calling Emit on the - tagMap : {} - context : {} - metricName : {} and tagKey : {}", tagMap, context,
             metricName, tagKey);
    emitMetrics(iterator, context, metricName, tagMap, tagKey);
  }

  private void populateApplicationTags(Map<String, String> tagMap, List<String> contextParts,
                                       List<String> targetTagList, String context) {
    tagMap.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
    for (int i = 0; i < contextParts.size(); i++) {
      if (i == targetTagList.size()) {
        LOG.info(" Context longer than targetTagList" + context);
        break;
      }
      if (targetTagList.get(i).equals(TYPE)) {
        continue;
      }
      tagMap.put(targetTagList.get(i), contextParts.get(i));
    }
  }

  private void sendMetrics(MetricStore store, Map<String, String> tags, String metricName, int timeStamp,
                           long value, MetricType gauge) throws Exception {
    LOG.info("Storing Metric - Tags {} metricName {} value {}", tags, metricName, value);
    store.add(new MetricValue(tags, metricName, timeStamp, value, gauge));
  }

  private MetricsTable getOrCreateMetricsTable(String tableName, DatasetProperties empty) {
    LOG.info("Get Metrics Table" + tableName);
    MetricsTable table = null;
    // metrics tables are in the system namespace
    Id.DatasetInstance metricsDatasetInstanceId = Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE, tableName);
    while (table == null) {
      try {
        table = DatasetsUtil.getOrCreateDataset(dsFramework, metricsDatasetInstanceId,
                                                MetricsTable.class.getName(), empty, null, null);
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
}
