/*
 * Copyright 2015-2018 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeDeleteQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeExploreQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.DimensionValue;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.dataset.lib.cube.Measurement;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.MetricsMessageId;
import co.cask.cdap.api.metrics.MetricsProcessorStatus;
import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.cube.Aggregation;
import co.cask.cdap.data2.dataset2.lib.cube.AggregationAlias;
import co.cask.cdap.data2.dataset2.lib.cube.DefaultAggregation;
import co.cask.cdap.data2.dataset2.lib.cube.DefaultCube;
import co.cask.cdap.data2.dataset2.lib.cube.FactTableSupplier;
import co.cask.cdap.data2.dataset2.lib.timeseries.FactTable;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.metrics.process.MetricsConsumerMetaTable;
import co.cask.cdap.metrics.process.TopicIdMetaKey;
import co.cask.cdap.metrics.process.TopicProcessMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link MetricStore}.
 */
public class DefaultMetricStore implements MetricStore {
  static final Map<String, Aggregation> AGGREGATIONS;

  private static final int TOTALS_RESOLUTION = Integer.MAX_VALUE;
  private static final String BY_NAMESPACE = "namespace";
  private static final String BY_APP = "app";
  private static final String BY_FLOW = "flow";
  private static final String BY_FLOWLET_QUEUE = "flow.queue";
  private static final String BY_MAPREDUCE = "mapreduce";
  private static final String BY_SERVICE = "service";
  private static final String BY_WORKER = "worker";
  private static final String BY_WORKFLOW = "workflow";
  private static final String BY_SPARK = "spark";
  private static final String BY_STREAM = "stream";
  private static final String BY_DATASET = "dataset";
  private static final String BY_PROFILE = "profile";
  private static final String BY_COMPONENT = "component";
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

    // flow
    aggs.put(BY_FLOW, new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.FLOW, Constants.Metrics.Tag.DATASET,
                       Constants.Metrics.Tag.RUN_ID, Constants.Metrics.Tag.FLOWLET,
                       Constants.Metrics.Tag.INSTANCE_ID, Constants.Metrics.Tag.FLOWLET_QUEUE),
      // i.e. for flows only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.FLOW)));
    // queue
    aggs.put(BY_FLOWLET_QUEUE, new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.FLOW, Constants.Metrics.Tag.CONSUMER,
                       Constants.Metrics.Tag.PRODUCER, Constants.Metrics.Tag.FLOWLET_QUEUE),
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.FLOW, Constants.Metrics.Tag.CONSUMER,
                       Constants.Metrics.Tag.PRODUCER, Constants.Metrics.Tag.FLOWLET_QUEUE)));
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
                       Constants.Metrics.Tag.RUN_ID, Constants.Metrics.Tag.INSTANCE_ID),
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

    // Streams:
    aggs.put(BY_STREAM, new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.STREAM),
      // i.e. for streams only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.STREAM)));

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

    AGGREGATIONS = Collections.unmodifiableMap(aggs);
  }

  @Inject
  DefaultMetricStore(MetricDatasetFactory dsFactory, CConfiguration cConf) {
    this(dsFactory, new int[] {1, 60, 3600, TOTALS_RESOLUTION}, cConf);
  }

  // NOTE: should never be used apart from data migration during cdap upgrade
  private DefaultMetricStore(MetricDatasetFactory dsFactory, int resolutions[], CConfiguration cConf) {
    long secRetentionSecs = cConf.getLong(Constants.Metrics.RETENTION_SECONDS + Constants.Metrics.SECOND_RESOLUTION +
                                            Constants.Metrics.RETENTION_SECONDS_SUFFIX);
    long minRetentionSecs = cConf.getLong(Constants.Metrics.RETENTION_SECONDS + Constants.Metrics.MINUTE_RESOLUTION +
                                            Constants.Metrics.RETENTION_SECONDS_SUFFIX);
    long hourRetentionSecs = cConf.getLong(Constants.Metrics.RETENTION_SECONDS + Constants.Metrics.HOUR_RESOLUTION +
                                             Constants.Metrics.RETENTION_SECONDS_SUFFIX);
    this.resolutionTTLMap = ImmutableMap.of(1, secRetentionSecs, 60, minRetentionSecs, 3600, hourRetentionSecs);
    FactTableSupplier factTableSupplier = new FactTableSupplier() {
      @Override
      public FactTable get(int resolution, int ignoredRollTime) {
        // roll time will be taken from configuration todo: clean this up
        return dsFactory.getOrCreateFactTable(resolution);
      }
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
  public Map<Integer, Long> getCounts() {
    return cube.get().getCounts();
  }

  @Override
  public Map<Integer, Long> getWriteTime() {
    return cube.get().getWriteTime();
  }

  public Map<Integer, Long> getWriteTimeDB() {
    return cube.get().getWriteTimeDB();
  }

  public Map<Integer, Long> getReadTimeDB() {
    return cube.get().getReadTimeDB();
  }

  public Map<Integer, Long> getMapSizeDB() {
    return cube.get().getMapSizeDB();
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
    String aggregation = getAggregation(query);
    return new CubeQuery(aggregation, query.getStartTs(), query.getEndTs(),
                         query.getResolution(), query.getLimit(), query.getMetrics(),
                         query.getSliceByTags(), query.getGroupByTags(), query.getInterpolator());
  }

  @Nullable
  private String getAggregation(MetricDataQuery query) {
    // We mostly rely on auto-selection of aggregation during query (in which case null is returned from
    // this method). In some specific cases we need to help resolve the aggregation though.
    Set<String> tagNames = ImmutableSet.<String>builder()
      .addAll(query.getSliceByTags().keySet()).addAll(query.getGroupByTags()).build();
    if (tagNames.contains(Constants.Metrics.Tag.FLOW)) {
      // NOTE: BY_FLOWLET_QUEUE agg has only producer and consumer metrics
      if (tagNames.contains(Constants.Metrics.Tag.PRODUCER) || tagNames.contains(Constants.Metrics.Tag.CONSUMER)) {
        return BY_FLOWLET_QUEUE;
      } else {
        return BY_FLOW;
      }
    }
    return null;
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

  private List<DimensionValue> toTagValues(List<co.cask.cdap.api.metrics.TagValue> input) {
    return Lists.transform(input, new Function<co.cask.cdap.api.metrics.TagValue, DimensionValue>() {
      @Nullable
      @Override
      public DimensionValue apply(co.cask.cdap.api.metrics.TagValue input) {
        if (input == null) {
          // SHOULD NEVER happen
          throw new NullPointerException();
        }
        return new DimensionValue(input.getName(), input.getValue());
      }
    });
  }
}
