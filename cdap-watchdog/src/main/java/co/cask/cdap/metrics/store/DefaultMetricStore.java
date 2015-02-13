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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricTags;
import co.cask.cdap.metrics.store.cube.Aggregation;
import co.cask.cdap.metrics.store.cube.Cube;
import co.cask.cdap.metrics.store.cube.CubeExploreQuery;
import co.cask.cdap.metrics.store.cube.CubeFact;
import co.cask.cdap.metrics.store.cube.CubeQuery;
import co.cask.cdap.metrics.store.cube.DefaultAggregation;
import co.cask.cdap.metrics.store.cube.DefaultCube;
import co.cask.cdap.metrics.store.cube.FactTableSupplier;
import co.cask.cdap.metrics.store.cube.TimeSeries;
import co.cask.cdap.metrics.store.timeseries.FactTable;
import co.cask.cdap.metrics.store.timeseries.MeasureType;
import co.cask.cdap.metrics.store.timeseries.TagValue;
import co.cask.cdap.metrics.store.timeseries.TimeValue;
import co.cask.cdap.metrics.transport.MetricType;
import co.cask.cdap.metrics.transport.MetricValue;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DefaultMetricStore implements MetricStore {
  private static final String PROGRAM_LEVEL2 = "pr2";
  private static final String PROGRAM_LEVEL3 = "pr3";
  private static final String PROGRAM_LEVEL4 = "pr4";

  private final Supplier<Cube> cube;
  private final Map<String, String> tagMapping;

  @Inject
  public DefaultMetricStore(final MetricDatasetFactory dsFactory) {
    final FactTableSupplier factTableSupplier = new FactTableSupplier() {
      @Override
      public FactTable get(int resolution, int ignoredRollTime) {
        // roll time will be taken from configuration todo: clean this up
        return dsFactory.get(resolution);
      }
    };
    this.cube = Suppliers.memoize(new Supplier<Cube>() {
      @Override
      public Cube get() {
        // 1 sec, 1 min, 1 hour and "all time totals"
        return new DefaultCube(new int[] {1, 60, 3600, Integer.MAX_VALUE}, factTableSupplier, createAggregations());
      }
    });

    // NOTE: to reduce number of aggregations we rename some of the emitted tags to "canonical" names
    this.tagMapping = ImmutableMap.of(
      // flow
      MetricTags.FLOWLET.getCodeName(), PROGRAM_LEVEL2,
      MetricTags.FLOWLET_QUEUE.getCodeName(), PROGRAM_LEVEL3,
      // mapreduce
      MetricTags.MR_TASK_TYPE.getCodeName(), PROGRAM_LEVEL2,
      MetricTags.INSTANCE_ID.getCodeName(), PROGRAM_LEVEL3,
      // service
      MetricTags.SERVICE_RUNNABLE.getCodeName(), PROGRAM_LEVEL2
    );
  }

  private static List<Aggregation> createAggregations() {
    List<Aggregation> aggs = Lists.newLinkedList();

    // <cluster metrics>, e.g. storage used
    aggs.add(new DefaultAggregation(ImmutableList.of(
      MetricTags.NAMESPACE.getCodeName(), MetricTags.CLUSTER_METRICS.getCodeName())));

    // app, prg type, prg name, ...
    aggs.add(new DefaultAggregation(ImmutableList.of(
      MetricTags.NAMESPACE.getCodeName(), MetricTags.APP.getCodeName(),
      // todo: do we even need program type? seems like program name unique within app across program types
      MetricTags.PROGRAM_TYPE.getCodeName(), MetricTags.PROGRAM.getCodeName(), MetricTags.RUN_ID.getCodeName(),
      PROGRAM_LEVEL2, PROGRAM_LEVEL3, PROGRAM_LEVEL4, MetricTags.DATASET.getCodeName()),
                                    // i.e. for programs only
                                    ImmutableList.of(
      MetricTags.NAMESPACE.getCodeName(), MetricTags.APP.getCodeName(),
      MetricTags.PROGRAM_TYPE.getCodeName(), MetricTags.PROGRAM.getCodeName())));

    // component, handler, method
    aggs.add(new DefaultAggregation(ImmutableList.of(
      MetricTags.NAMESPACE.getCodeName(),
      MetricTags.COMPONENT.getCodeName(), MetricTags.HANDLER.getCodeName(), MetricTags.METHOD.getCodeName()),
                                    // i.e. for components only
                                    ImmutableList.of(
      MetricTags.NAMESPACE.getCodeName(), MetricTags.COMPONENT.getCodeName())));

    // component, handler, method, stream (for stream only) todo: seems like emitted context is wrong, review...
    aggs.add(new DefaultAggregation(ImmutableList.of(
      MetricTags.NAMESPACE.getCodeName(),
      MetricTags.COMPONENT.getCodeName(), MetricTags.HANDLER.getCodeName(), MetricTags.METHOD.getCodeName(),
      MetricTags.STREAM.getCodeName()),
                                    // i.e. for stream only
                                    ImmutableList.of(MetricTags.STREAM.getCodeName())));

    // dataset
    aggs.add(new DefaultAggregation(ImmutableList.of(MetricTags.NAMESPACE.getCodeName(),
                                                     MetricTags.DATASET.getCodeName()),
                                    // i.e. for datasets only
                                    ImmutableList.of(MetricTags.DATASET.getCodeName())));

    return aggs;
  }

  @Override
  public void add(MetricValue metricValue) throws Exception {
    String scope = metricValue.getTags().get(MetricTags.SCOPE);
    String measureName = (scope == null ? "system." : scope + ".") + metricValue.getName();
    Map<String, String> expandedTagNames = expandTagNames(metricValue.getTags());
    CubeFact fact = new CubeFact(replaceTagsIfNeeded(expandedTagNames),
                                 toMeasureType(metricValue.getType()), measureName,
                                 new TimeValue(metricValue.getTimestamp(), metricValue.getValue()));
    cube.get().add(fact);
  }

  private Map<String, String> expandTagNames(Map<String, String> tagValues) {
    Map<String, String> result = Maps.newHashMap();
    for (Map.Entry<String, String> tagValue : tagValues.entrySet()) {
      String tagNameReplacement = MetricTags.valueOfCodeName(tagValue.getKey());
      result.put(tagNameReplacement == null ? tagValue.getKey() : tagNameReplacement, tagValue.getValue());
    }
    return result;
  }

  @Override
  public Collection<TimeSeries> query(CubeQuery query) throws Exception {
    CubeQuery q =
      new CubeQuery(query, replaceTagsIfNeeded(query.getSliceByTags()), replaceTagsIfNeeded(query.getGroupByTags()));
    Collection<TimeSeries> cubeResult = cube.get().query(q);
    List<TimeSeries> result = Lists.newArrayList();
    for (TimeSeries timeSeries : cubeResult) {
      result.add(new TimeSeries(timeSeries, replaceTagsIfNeeded(timeSeries.getTagValues())));
    }
    return result;
  }

  @Override
  public void deleteBefore(long timestamp) {
    // todo: implement metric ttl
  }

  private void replaceTagValuesIfNeeded(List<TagValue> tagValues) {
    for (int i = 0; i < tagValues.size(); i++) {
      TagValue tagValue = tagValues.get(i);
      String tagNameReplacement = tagMapping.get(tagValue.getTagName());
      if (tagNameReplacement != null) {
        tagValues.set(i, new TagValue(tagNameReplacement, tagValue.getValue()));
      }
    }
  }

  @Override
  public Collection<TagValue> findNextAvailableTags(CubeExploreQuery query) throws Exception {
    replaceTagValuesIfNeeded(query.getTagValues());
    return cube.get().findNextAvailableTags(query);
  }

  @Override
  public Collection<String> findMetricNames(CubeExploreQuery query) throws Exception {
    replaceTagValuesIfNeeded(query.getTagValues());
    return cube.get().getMeasureNames(query);
  }

  private Map<String, String> replaceTagsIfNeeded(Map<String, String> tagValues) {
    // replace emitted tag names to the ones expected by aggregations
    Map<String, String> result = Maps.newHashMap();
    for (Map.Entry<String, String> tagValue : tagValues.entrySet()) {
      String tagNameReplacement = tagMapping.get(tagValue.getKey());
      result.put(tagNameReplacement == null ? tagValue.getKey() : tagNameReplacement, tagValue.getValue());
    }
    return result;
  }

  private List<String> replaceTagsIfNeeded(List<String> tagNames) {
    // replace emitted tag names to the ones expected by aggregations
    List<String> result = Lists.newArrayList();
    for (String tagName : tagNames) {
      String tagNameReplacement = tagMapping.get(tagName);
      result.add(tagNameReplacement == null ? tagName : tagNameReplacement);
    }
    return result;
  }

  private MeasureType toMeasureType(MetricType type) {
    switch (type) {
      case COUNTER:
        return MeasureType.COUNTER;
      case GAUGE:
        return MeasureType.GAUGE;
      default:
        // should never happen
        throw new IllegalArgumentException("Unknown MetricType: " + type);
    }
  }
}
