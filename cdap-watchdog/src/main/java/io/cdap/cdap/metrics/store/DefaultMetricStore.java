/*
 * Copyright 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.metrics.store;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.cube.Cube;
import io.cdap.cdap.api.dataset.lib.cube.CubeDeleteQuery;
import io.cdap.cdap.api.dataset.lib.cube.CubeExploreQuery;
import io.cdap.cdap.api.dataset.lib.cube.CubeFact;
import io.cdap.cdap.api.dataset.lib.cube.CubeQuery;
import io.cdap.cdap.api.dataset.lib.cube.DimensionValue;
import io.cdap.cdap.api.dataset.lib.cube.MeasureType;
import io.cdap.cdap.api.dataset.lib.cube.Measurement;
import io.cdap.cdap.api.dataset.lib.cube.TimeSeries;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricDeleteQuery;
import io.cdap.cdap.api.metrics.MetricSearchQuery;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.MetricsMessageId;
import io.cdap.cdap.api.metrics.MetricsProcessorStatus;
import io.cdap.cdap.api.metrics.TagValue;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.lib.cube.Aggregation;
import io.cdap.cdap.data2.dataset2.lib.cube.AggregationAlias;
import io.cdap.cdap.data2.dataset2.lib.cube.DefaultAggregation;
import io.cdap.cdap.data2.dataset2.lib.cube.DefaultCube;
import io.cdap.cdap.data2.dataset2.lib.cube.FactTableSupplier;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.metrics.process.MetricsConsumerMetaTable;
import io.cdap.cdap.metrics.process.TopicIdMetaKey;
import io.cdap.cdap.metrics.process.TopicProcessMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link MetricStore}.
 */
public class DefaultMetricStore implements MetricStore {
  public static final Map<String, Aggregation> AGGREGATIONS;

  private static final int TOTALS_RESOLUTION = Integer.MAX_VALUE;
  private static final String BY_NAMESPACE = "namespace";
  private static final String BY_APP = "app";
  private static final String BY_MAPREDUCE = "mapreduce";
  private static final String BY_SERVICE = "service";
  private static final String BY_WORKER = "worker";
  private static final String BY_WORKFLOW = "workflow";
  private static final String BY_SPARK = "spark";
  private static final String BY_DATASET = "dataset";
  private static final String BY_PROFILE = "profile";
  private static final String BY_COMPONENT = "component";
  private static final String BY_PLUGIN = "plugin";
  private static final Map<String, AggregationAlias> AGGREGATIONS_ALIAS_DIMENSIONS =
    ImmutableMap.of(BY_WORKFLOW,
                    new AggregationAlias(ImmutableMap.of(Constants.Metrics.Tag.RUN_ID,
                                                         Constants.Metrics.Tag.WORKFLOW_RUN_ID)));

  private final Supplier<Cube> cube;
  private final Supplier<MetricsConsumerMetaTable> metaTableSupplier;
  private MetricsContext metricsContext;
  private final List<TopicId> metricsTopics;
  private final Map<Integer, Long> resolutionTTLMap;


  static {
    // NOTE: changing aggregations will require more work than just changing the below code. See CDAP-1466 for details.
    // Note that adding or delete aggregations should not affect the metrics
    // but modifying the existing aggregations will make the metrics incorrect since it will change the way we
    // store and query metrics
    Map<String, Aggregation> aggs = Maps.newHashMap();

    // Namespaces:
    aggs.put(BY_NAMESPACE, new DefaultAggregation(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE)));

    // Applications:
    aggs.put(BY_APP, new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP, Constants.Metrics.Tag.DATASET),
      // i.e. for programs only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP)));

    // Programs:

    // Note that dataset tag goes before runId and such. This is a trade-off between efficiency of two query types:
    // * program metrics
    // * dataset metrics per program
    // It makes the former a bit slower, but bearable, as program usually doesn't access many datasets. While it speeds
    // up the latter significantly, otherwise (if dataset tag is after runId and such) queries like
    // "writes into dataset A per program" would be potentially scannig thru whole program history.

    // mapreduce
    aggs.put(BY_MAPREDUCE, new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.MAPREDUCE, Constants.Metrics.Tag.DATASET,
                       Constants.Metrics.Tag.RUN_ID, Constants.Metrics.Tag.MR_TASK_TYPE,
                       Constants.Metrics.Tag.INSTANCE_ID),
      // i.e. for mapreduce only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.MAPREDUCE)));
    // service
    aggs.put(BY_SERVICE, new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.SERVICE, Constants.Metrics.Tag.DATASET,
                       Constants.Metrics.Tag.RUN_ID, Constants.Metrics.Tag.HANDLER,
                       Constants.Metrics.Tag.METHOD, Constants.Metrics.Tag.INSTANCE_ID,
                       Constants.Metrics.Tag.THREAD),
      // i.e. for service only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.SERVICE)));

    // worker
    aggs.put(BY_WORKER, new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.WORKER, Constants.Metrics.Tag.DATASET,
                       Constants.Metrics.Tag.RUN_ID, Constants.Metrics.Tag.INSTANCE_ID,
                       Constants.Metrics.Tag.PROGRAM_ENTITY),
      // i.e. for worker only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.WORKER)));

    // workflow
    aggs.put(BY_WORKFLOW, new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.WORKFLOW, Constants.Metrics.Tag.DATASET,
                       Constants.Metrics.Tag.RUN_ID, Constants.Metrics.Tag.NODE),
      // i.e. for workflow only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.WORKFLOW)));

    // spark
    aggs.put(BY_SPARK, new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.SPARK, Constants.Metrics.Tag.DATASET,
                       Constants.Metrics.Tag.RUN_ID),
      // i.e. for spark only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.SPARK)));

    // Datasets:
    aggs.put(BY_DATASET, new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.DATASET),
      // i.e. for datasets only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.DATASET)));

    // Profiles:
    aggs.put(BY_PROFILE, new DefaultAggregation(
      // The required dimension for profile is the scope and profile name
      // These tags are ordered because of the efficiency, since we only have limited number program types, so it
      // comes before the app
      ImmutableList.of(Constants.Metrics.Tag.PROFILE_SCOPE, Constants.Metrics.Tag.PROFILE,
                       Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.PROGRAM_TYPE,
                       Constants.Metrics.Tag.APP, Constants.Metrics.Tag.PROGRAM,
                       Constants.Metrics.Tag.RUN_ID),
      ImmutableList.of(Constants.Metrics.Tag.PROFILE_SCOPE, Constants.Metrics.Tag.PROFILE)));

    // System components:
    aggs.put(BY_COMPONENT, new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.COMPONENT,
                       Constants.Metrics.Tag.HANDLER, Constants.Metrics.Tag.METHOD),
      // i.e. for components only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.COMPONENT)));

    // Plugins:
    aggs.put(BY_PLUGIN, new DefaultAggregation(
      // Plugin name and plugin type are required dimensions as they uniquely identify a plugin
      ImmutableList.of(Constants.Metrics.Tag.PLUGIN_NAME, Constants.Metrics.Tag.PLUGIN_TYPE),
      ImmutableList.of(Constants.Metrics.Tag.PLUGIN_NAME, Constants.Metrics.Tag.PLUGIN_TYPE)));

    AGGREGATIONS = Collections.unmodifiableMap(aggs);
  }

  @Inject
  DefaultMetricStore(MetricDatasetFactory dsFactory, CConfiguration cConf) {
    int minimumResolution = cConf.getInt(Constants.Metrics.METRICS_MINIMUM_RESOLUTION_SECONDS);
    int[] resolutions = minimumResolution < 60 ?
      new int[] {minimumResolution, 60, 3600, TOTALS_RESOLUTION} : new int[] {60, 3600, TOTALS_RESOLUTION};
    long minRetentionSecs = cConf.getLong(Constants.Metrics.RETENTION_SECONDS + Constants.Metrics.MINUTE_RESOLUTION +
                                            Constants.Metrics.RETENTION_SECONDS_SUFFIX);
    long hourRetentionSecs = cConf.getLong(Constants.Metrics.RETENTION_SECONDS + Constants.Metrics.HOUR_RESOLUTION +
                                             Constants.Metrics.RETENTION_SECONDS_SUFFIX);
    ImmutableMap.Builder<Integer, Long> builder = ImmutableMap.<Integer, Long>builder()
      .put(60, minRetentionSecs)
      .put(3600, hourRetentionSecs);
    if (minimumResolution < 60) {
      builder.put(minimumResolution, cConf.getLong(Constants.Metrics.MINIMUM_RESOLUTION_RETENTION_SECONDS));
    }
    this.resolutionTTLMap = builder.build();
    FactTableSupplier factTableSupplier = (resolution, ignoredRollTime) -> {
      // roll time will be taken from configuration
      // TODO: remove roll time from the supplier api, https://issues.cask.co/browse/CDAP-14730
      return dsFactory.getOrCreateFactTable(resolution);
    };
    this.cube = Suppliers.memoize(new Supplier<Cube>() {
      @Override
      public Cube get() {
        DefaultCube cube = new DefaultCube(resolutions, factTableSupplier, AGGREGATIONS, AGGREGATIONS_ALIAS_DIMENSIONS);
        cube.setMetricsCollector(metricsContext);
        return cube;
      }
    });

    this.metaTableSupplier = Suppliers.memoize(dsFactory::createConsumerMeta);
    int topicNumbers = cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM);
    String topicPrefix = cConf.get(Constants.Metrics.TOPIC_PREFIX);
    metricsTopics = new ArrayList<>();
    for (int i = 0; i < topicNumbers; i++) {
      this.metricsTopics.add(NamespaceId.SYSTEM.topic(topicPrefix + i));
    }
  }

  @Override
  public void setMetricsContext(MetricsContext metricsContext) {
    this.metricsContext = metricsContext;
  }
  
  @Override
  public void add(MetricValues metricValues) {
    add(ImmutableList.of(metricValues));
  }

  @Override
  public void add(Collection<? extends MetricValues> metricValues) {
    List<CubeFact> facts = Lists.newArrayListWithCapacity(metricValues.size());
    for (MetricValues metricValue : metricValues) {
      String scope = metricValue.getTags().get(Constants.Metrics.Tag.SCOPE);
      List<Measurement> metrics = Lists.newArrayList();
      // todo improve this logic?
      for (MetricValue metric : metricValue.getMetrics()) {
        String measureName = (scope == null ? "system." : scope + ".") + metric.getName();
        MeasureType type = metric.getType() == MetricType.COUNTER ? MeasureType.COUNTER : MeasureType.GAUGE;
        metrics.add(new Measurement(measureName, type, metric.getValue()));
      }

      CubeFact fact = new CubeFact(metricValue.getTimestamp())
        .addDimensionValues(metricValue.getTags())
        .addMeasurements(metrics);
      facts.add(fact);
    }
    cube.get().add(facts);
  }

  @Override
  public Collection<MetricTimeSeries> query(MetricDataQuery query) {
    Collection<TimeSeries> cubeResult = cube.get().query(buildCubeQuery(query));
    List<MetricTimeSeries> result = Lists.newArrayList();
    for (TimeSeries timeSeries : cubeResult) {
      result.add(new MetricTimeSeries(timeSeries.getMeasureName(),
                                      timeSeries.getDimensionValues(),
                                      timeSeries.getTimeValues()));
    }
    return result;
  }

  private CubeQuery buildCubeQuery(MetricDataQuery query) {
    return new CubeQuery(null, query.getStartTs(), query.getEndTs(),
                         query.getResolution(), query.getLimit(), query.getMetrics(),
                         query.getSliceByTags(), query.getGroupByTags(), query.getAggregationOption(),
                         query.getInterpolator());
  }

  @Override
  public void deleteBefore(long timestamp) {
    for (int resolution : resolutionTTLMap.keySet()) {
      // Delete all data before the timestamp. null for MeasureName indicates match any MeasureName.
      deleteMetricsBeforeTimestamp(timestamp, resolution);
    }
  }

  @Override
  public void deleteTTLExpired() {
    long currentTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    for (Map.Entry<Integer, Long> resolutionTTL : resolutionTTLMap.entrySet()) {
      deleteMetricsBeforeTimestamp(currentTime - resolutionTTL.getValue(), resolutionTTL.getKey());
    }
  }

  @Override
  public void delete(MetricDeleteQuery query) {
    cube.get().delete(buildCubeDeleteQuery(query));
  }

  @Override
  public void deleteAll() {
    // this will delete all aggregates metrics data
    delete(new MetricDeleteQuery(0, System.currentTimeMillis() / 1000, Collections.emptySet(),
                                 Collections.emptyMap(), Collections.emptyList()));
    // this will delete all timeseries data
    deleteBefore(System.currentTimeMillis() / 1000);
  }

  private CubeDeleteQuery buildCubeDeleteQuery(MetricDeleteQuery query) {
    // note: delete query currently usually executed synchronously,
    //       so we only attempt to delete totals, to avoid timeout
    return new CubeDeleteQuery(query.getStartTs(), query.getEndTs(), TOTALS_RESOLUTION,
                               query.getSliceByTags(), query.getMetricNames(), query.getTagPredicate());
  }

  @Override
  public Collection<TagValue> findNextAvailableTags(MetricSearchQuery query) {
    Collection<DimensionValue> tags = cube.get().findDimensionValues(buildCubeSearchQuery(query));
    Collection<TagValue> result = Lists.newArrayList();
    for (DimensionValue dimensionValue : tags) {
      result.add(new TagValue(dimensionValue.getName(), dimensionValue.getValue()));
    }
    return result;
  }

  private CubeExploreQuery buildCubeSearchQuery(MetricSearchQuery query) {
    return new CubeExploreQuery(query.getStartTs(), query.getEndTs(), query.getResolution(),
                                query.getLimit(), toTagValues(query.getTagValues()));
  }

  @Override
  public Collection<String> findMetricNames(MetricSearchQuery query) {
    return cube.get().findMeasureNames(buildCubeSearchQuery(query));
  }

  /**
   * Read the metrics processing stats from meta table and return the map of topic information to stats
   * @return Map of topic to metrics processing stats
   * @throws Exception
   */
  @Override
  public Map<String, MetricsProcessorStatus> getMetricsProcessorStats() throws Exception {
    MetricsConsumerMetaTable metaTable = metaTableSupplier.get();
    Map<String, MetricsProcessorStatus> processMap = new HashMap<>();
    for (TopicId topicId : metricsTopics) {
      TopicProcessMeta topicProcessMeta = metaTable.getTopicProcessMeta(new TopicIdMetaKey(topicId));
      if (topicProcessMeta != null) {
        MessageId messageId = new MessageId(topicProcessMeta.getMessageId());
        MetricsMessageId metricsMessageId = new MetricsMessageId(messageId.getPublishTimestamp(),
                                                                 messageId.getSequenceId(),
                                                                 messageId.getPayloadWriteTimestamp(),
                                                                 messageId.getPayloadSequenceId());
        processMap.put(
          topicId.getTopic(), new MetricsProcessorStatus(metricsMessageId,
                                                         topicProcessMeta.getOldestMetricsTimestamp(),
                                                         topicProcessMeta.getLatestMetricsTimestamp(),
                                                         topicProcessMeta.getMessagesProcessed(),
                                                         topicProcessMeta.getLastProcessedTimestamp()));
      }
    }
    return processMap;
  }

  private void deleteMetricsBeforeTimestamp(long timestamp, int resolution) {
    CubeDeleteQuery query = new CubeDeleteQuery(0, timestamp, resolution, Collections.emptyMap(),
                                                Collections.emptySet(), strings -> true);
    cube.get().delete(query);
  }

  private List<DimensionValue> toTagValues(List<io.cdap.cdap.api.metrics.TagValue> input) {
    return Lists.transform(input, new Function<io.cdap.cdap.api.metrics.TagValue, DimensionValue>() {
      @Nullable
      @Override
      public DimensionValue apply(io.cdap.cdap.api.metrics.TagValue input) {
        if (input == null) {
          // SHOULD NEVER happen
          throw new NullPointerException();
        }
        return new DimensionValue(input.getName(), input.getValue());
      }
    });
  }
}
