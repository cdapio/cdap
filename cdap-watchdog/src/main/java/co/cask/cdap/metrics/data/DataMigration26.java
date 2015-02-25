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
package co.cask.cdap.metrics.data;

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
import co.cask.cdap.metrics.MetricsConstants;
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
 * Data migration class
 */
public class DataMigration26 {

  private static final Logger LOG = LoggerFactory.getLogger(DataMigration26.class);
  private static final String ENTITY_TABLE_NAME_SUFFIX = ".metrics.entity";
  private static final String AGGREGATES_TABLE_NAME_SUFFIX = ".metrics.table.agg";
  private static final String TIMESERIES_SECOND_TABLE_NAME_SUFFIX = ".metrics.table.ts.1";


  private final DatasetFramework dsFramework;
  private final MetricStore aggMetricStore;
  private final MetricStore timeSeriesStore;
  private final List<String> scopes = ImmutableList.of("system", "user");
  private final Map<String, List<String>> oldFormatMapping = ImmutableMap.<String, List<String>>of(
    "f", ImmutableList.of(Constants.Metrics.Tag.APP,
                          "type",
                          Constants.Metrics.Tag.FLOW,
                          Constants.Metrics.Tag.FLOWLET,
                          Constants.Metrics.Tag.INSTANCE_ID

    ),
    "b", ImmutableList.of(Constants.Metrics.Tag.APP,
                          "type",
                          Constants.Metrics.Tag.MAPREDUCE,
                          Constants.Metrics.Tag.MR_TASK_TYPE,
                          Constants.Metrics.Tag.INSTANCE_ID),
    "p", ImmutableList.of(Constants.Metrics.Tag.APP,
                          "type",
                          Constants.Metrics.Tag.PROCEDURE,
                          Constants.Metrics.Tag.INSTANCE_ID
    ),
    "s", ImmutableList.of(Constants.Metrics.Tag.APP,
                          "type",
                          Constants.Metrics.Tag.SPARK,
                          Constants.Metrics.Tag.INSTANCE_ID),
    "u", ImmutableList.of(Constants.Metrics.Tag.APP,
                          "type",
                          Constants.Metrics.Tag.SERVICE,
                          Constants.Metrics.Tag.SERVICE_RUNNABLE,
                          Constants.Metrics.Tag.INSTANCE_ID));

  List<String> datasetMetrics = ImmutableList.of("store.ops", "store.reads", "store.writes");

  private final Map<String, Map<String, String>> mapOldSystemContextToNew =
    ImmutableMap.<String, Map<String, String>>of(
      "transactions", ImmutableMap.<String, String>of(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE,
                                                      Constants.Metrics.Tag.COMPONENT, "transactions"),
      "-", ImmutableMap.<String, String>of(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE),

      "gateway", ImmutableMap.<String, String>of(Constants.Metrics.Tag.COMPONENT, Constants.Gateway.METRICS_CONTEXT,
                                                 Constants.Metrics.Tag.HANDLER, Constants.Gateway.STREAM_HANDLER_NAME)
    );


  private Map<String, MetricsTable> scopeToAggregatesTable = Maps.newHashMap();
  private Map<String, EntityTable> scopeToEntityTable = Maps.newHashMap();
  private Map<String, MetricsTable> scopeToTimeseriesTable = Maps.newHashMap();
  private Map<String, MetricsEntityCodec> scopeToCodec = Maps.newHashMap();


  public DataMigration26(final CConfiguration cConf, final DatasetFramework dsFramework,
                         DefaultMetricDatasetFactory factory) {

    //todo - change hard-coded table name to read from configuration instead.
    System.out.println("Initializing Data Migration.");
    this.dsFramework = dsFramework;
    for (String scope : scopes) {
      scopeToEntityTable.put(scope, new EntityTable(getOrCreateMetricsTable(scope + ENTITY_TABLE_NAME_SUFFIX,
                                                                            DatasetProperties.EMPTY)));
      scopeToAggregatesTable.put(scope, getOrCreateMetricsTable(scope + AGGREGATES_TABLE_NAME_SUFFIX,
                                                                DatasetProperties.EMPTY));

      scopeToTimeseriesTable.put(scope, getOrCreateMetricsTable(scope + TIMESERIES_SECOND_TABLE_NAME_SUFFIX,
                                                                DatasetProperties.EMPTY));
      scopeToCodec.put(scope, new MetricsEntityCodec(scopeToEntityTable.get(scope),
                                                     MetricsConstants.DEFAULT_CONTEXT_DEPTH,
                                                     MetricsConstants.DEFAULT_METRIC_DEPTH,
                                                     MetricsConstants.DEFAULT_TAG_DEPTH));
    }
    aggMetricStore = new DefaultMetricStore(factory, new int[]{Integer.MAX_VALUE});
    timeSeriesStore = new DefaultMetricStore(factory, new int[]{1, 60, 3600});
  }

  public void decodeTimeSeriesTable26() {
    try {
      for (String scope : scopes) {
        Scanner scanner = scopeToTimeseriesTable.get(scope).scan(null, null, null, null);
        MetricsEntityCodec codec = scopeToCodec.get(scope);
        System.out.println("Decoding for scope " + scope + "on table " + scopeToTimeseriesTable.get(scope) +
                             " using codec " + codec.toString());
        Row row;
        while ((row = scanner.next()) != null) {
          byte[] rowKey = row.getRow();
          int offset = 0;
          String context = codec.decode26(MetricsEntityType.CONTEXT, rowKey, offset);
          offset += codec.getEncodedSize26(MetricsEntityType.CONTEXT);
          String metricName = codec.decode26(MetricsEntityType.METRIC, rowKey, offset);
          offset += codec.getEncodedSize26(MetricsEntityType.METRIC);
          String tagName = codec.decode26(MetricsEntityType.TAG, rowKey, offset);
          offset += codec.getEncodedSize26(MetricsEntityType.TAG);
          int timeBase = Bytes.toInt(rowKey, offset, 4);
          // Then it's the runId
          offset += 4;
          String runId = codec.decode26(MetricsEntityType.RUN, rowKey, offset);
          constructMetricTimeValue(scope, context, metricName,  tagName, timeBase, runId,
                                   row.getColumns().entrySet().iterator());
        }
      }
    }  catch (Exception e) {
      LOG.info("Exception during data-transfer in aggregates table", e);
      // no-op
    }
  }

  private void constructMetricTimeValue(String scope, String context, String metricName, String tagName,
                                        int timeBase, String runId,
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

    if (systemMap != null) {
      tagMap.putAll(systemMap);
      if (contextParts.size() > 1) {
        if (contextParts.get(1).equals("dataset") && !tagName.equals(MetricsConstants.EMPTY_TAG)) {
          tagMap.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
          tagMap.put(Constants.Metrics.Tag.DATASET, tagName);
        } else if (contextParts.get(1).equals("stream")) {
          if (tagName.equals(MetricsConstants.EMPTY_TAG)) {
            tagMap.put(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE);
          } else {
            tagMap.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
            tagMap.put(Constants.Metrics.Tag.STREAM, tagName);
          }
        }
      }
    } else {
      if (contextParts.size() > 1) {
        List<String> targetTagList = oldFormatMapping.get(contextParts.get(1));
        if (targetTagList != null) {
          tagMap.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
          for (int i = 0; i < contextParts.size(); i++) {
            if (i == targetTagList.size()) {
              System.out.println(" Context longer than targetTagList" + context);
              break;
            }
            if (targetTagList.get(i).equals("type")) {
              continue;
            }
            tagMap.put(targetTagList.get(i), contextParts.get(i));
          }
          if (!tagName.equals(MetricsConstants.EMPTY_TAG) && datasetMetrics.contains(metricName)) {
            tagMap.put(Constants.Metrics.Tag.DATASET, tagName);
          } else if (!tagName.equals(MetricsConstants.EMPTY_TAG) && contextParts.get(1).equals("f")) {
            //queue
            tagMap.put(Constants.Metrics.Tag.FLOWLET_QUEUE, tagName);
          }
        }
      }  else {
        System.out.println("unmatched" + context);
        return;
      }
    }
    System.out.println("Matched Context " + context + " with tagMappings" + tagMap);
    /*while (iterator.hasNext()) {
      Map.Entry<byte[], byte[]> entry = iterator.next();
      sendMetrics(timeSeriesStore, tagMap, metricName, timeBase + Bytes.toInt(entry.getKey()),
                  Bytes.toLong(entry.getValue()), MetricType.GAUGE);
    }*/
  }

  public void decodeAggregatesTable26() {
    try {
      for (String scope : scopes) {
        Scanner scanner = scopeToAggregatesTable.get(scope).scan(null, null, null, null);
        MetricsEntityCodec codec = scopeToCodec.get(scope);
        System.out.println("Decoding for scope " + scope + "on table " + scopeToAggregatesTable.get(scope).toString() +
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
    }  catch (Exception e) {
      LOG.info("Exception during data-transfer in aggregates table", e);
      // no-op
    }
  }

  private long getAggregateValue(Iterator<Map.Entry<byte[], byte[]>> iterator) {
    long result = 0;
    if (iterator != null) {
      while (iterator.hasNext()) {
        result += Bytes.toLong(iterator.next().getValue());
      }
    }
    return result;
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

    if (systemMap != null) {
      tagMap.putAll(systemMap);
      // iterate the tags,for each tag name create map with the key "dataset" and name "tagName" and use the value.
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        if (contextParts.size() > 1) {
          if (contextParts.get(1).equals("dataset") &&
            !Bytes.toString(entry.getKey()).equals(MetricsConstants.EMPTY_TAG)) {
            Map<String, String> newMap = Maps.newHashMap(tagMap);
            newMap.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
            newMap.put(Constants.Metrics.Tag.DATASET, Bytes.toString(entry.getKey()));
            sendMetrics(aggMetricStore, newMap, metricName, 0, Bytes.toLong(entry.getValue()), MetricType.GAUGE);
            //LOG.info("Dataset Tag Mappings : {}", newMap);
          } else if (contextParts.get(1).equals("stream")) {
            Map<String, String> newMap = Maps.newHashMap(tagMap);
            // scan the tags, if tag is null ,
            // we emit with system namespace, else we emit with default namespace and tag as stream-name
            if (Bytes.toString(entry.getKey()).equals(MetricsConstants.EMPTY_TAG)) {
              newMap.put(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE);
            } else {
              newMap.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
              newMap.put(Constants.Metrics.Tag.STREAM, Bytes.toString(entry.getKey()));
              //LOG.info("Stream Tag Mappings : complete context : {}", context);
            }
            sendMetrics(aggMetricStore, newMap, metricName, 0, Bytes.toLong(entry.getValue()), MetricType.GAUGE);
          } else {
            sendMetrics(aggMetricStore, tagMap, metricName, 0, Bytes.toLong(entry.getValue()), MetricType.GAUGE);
          }
        } else {
          sendMetrics(aggMetricStore, tagMap, metricName, 0, Bytes.toLong(entry.getValue()), MetricType.GAUGE);
        }
      }
    } else  if (contextParts.size() > 1) {
      // application data
      List<String> targetTagList = oldFormatMapping.get(contextParts.get(1));
      if (targetTagList != null) {
        tagMap.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
        for (int i = 0; i < contextParts.size(); i++) {
          if (i == targetTagList.size()) {
            System.out.println(" Context longer than targetTagList" + context);
            break;
          }
          if (targetTagList.get(i).equals("type")) {
            continue;
          }
          tagMap.put(targetTagList.get(i), contextParts.get(i));
        }
        // check tags - if we have tags, check if metric name is one of the dataset metrics -> if so, use dataset
        // else check if type is flow, then -> use queues
        while (iterator.hasNext()) {
          Map.Entry<byte[], byte[]> entry = iterator.next();
          String tag = Bytes.toString(entry.getKey());
          if (!tag.equals(MetricsConstants.EMPTY_TAG) && datasetMetrics.contains(metricName)) {
            Map<String, String> newMap = Maps.newHashMap(tagMap);
            newMap.put(Constants.Metrics.Tag.DATASET, tag);
            sendMetrics(aggMetricStore, newMap, metricName, 0, Bytes.toLong(entry.getValue()), MetricType.GAUGE);
            //LOG.info("Dataset Tag Mappings : {}", newMap);
          } else if (!tag.equals(MetricsConstants.EMPTY_TAG) && contextParts.get(1).equals("f")) {
            //queue
            Map<String, String> newMap = Maps.newHashMap(tagMap);
            newMap.put(Constants.Metrics.Tag.FLOWLET_QUEUE, tag);
            sendMetrics(aggMetricStore, newMap, metricName, 0, Bytes.toLong(entry.getValue()), MetricType.GAUGE);
            //LOG.info("Queue Tag Mappings : {}", newMap);
          } else {
            // emit the metric for empty tag
            sendMetrics(aggMetricStore, tagMap, metricName, 0, Bytes.toLong(entry.getValue()), MetricType.GAUGE);
          }
        }
      } else {
        // service metrics , we are skipping them as they were not exposed for querying before
        LOG.info("Unmatched - context {} metric {}", context, metricName);
      }
    } else {
      // should not reach here
      LOG.info("Shouldn't reach here - app - context {} metric {}", context, metricName);
    }
  }

  private void sendMetrics(MetricStore store, Map<String, String> tags, String metricName, int timeStamp,
                           long value, MetricType gauge) throws Exception {
    store.add(new MetricValue(tags, metricName, timeStamp, value, gauge));
  }

  private MetricsTable getOrCreateMetricsTable(String tableName, DatasetProperties empty) {
    System.out.println("Get Metrics Table" + tableName);
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
