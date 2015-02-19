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

import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.api.metrics.TimeValue;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.metrics.store.cube.Aggregation;
import co.cask.cdap.metrics.store.cube.Cube;
import co.cask.cdap.metrics.store.cube.CubeDeleteQuery;
import co.cask.cdap.metrics.store.cube.CubeExploreQuery;
import co.cask.cdap.metrics.store.cube.CubeFact;
import co.cask.cdap.metrics.store.cube.CubeQuery;
import co.cask.cdap.metrics.store.cube.DefaultAggregation;
import co.cask.cdap.metrics.store.cube.DefaultCube;
import co.cask.cdap.metrics.store.cube.FactTableSupplier;
import co.cask.cdap.metrics.store.cube.TimeSeries;
import co.cask.cdap.metrics.store.timeseries.FactTable;
import co.cask.cdap.metrics.store.timeseries.MeasureType;
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
import javax.annotation.Nullable;

/**
 *
 */
public class DefaultMetricStore implements MetricStore {
  private static final String PROGRAM_LEVEL2 = "pr2";
  private static final String PROGRAM_LEVEL3 = "pr3";
  private static final String PROGRAM_LEVEL4 = "pr4";

  private final Supplier<Cube> cube;
  private final Map<String, String> tagMapping;
  private final Map<String, Map<String, String>> reverseMapping;

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
      Constants.Metrics.Tag.FLOWLET, PROGRAM_LEVEL2,
      Constants.Metrics.Tag.FLOWLET_QUEUE, PROGRAM_LEVEL3,
      // mapreduce
      Constants.Metrics.Tag.MR_TASK_TYPE, PROGRAM_LEVEL2,
      Constants.Metrics.Tag.INSTANCE_ID, PROGRAM_LEVEL3,
      // service
      Constants.Metrics.Tag.SERVICE_RUNNABLE, PROGRAM_LEVEL2
    );

    // reverse mapping
    // todo : remove hard-coded values and use TypeId (need to move TypeId out of app-fabric ?)
    this.reverseMapping = ImmutableMap.<String, Map<String, String>>of(
      //flow
      "f", ImmutableMap.<String, String>of(PROGRAM_LEVEL2, Constants.Metrics.Tag.FLOWLET,
                                              PROGRAM_LEVEL3, Constants.Metrics.Tag.FLOWLET_QUEUE)
      ,
      "b", ImmutableMap.<String, String>of(PROGRAM_LEVEL2, Constants.Metrics.Tag.MR_TASK_TYPE,
                                                   PROGRAM_LEVEL3, Constants.Metrics.Tag.INSTANCE_ID)
      ,
      "u", ImmutableMap.<String, String>of(PROGRAM_LEVEL2, Constants.Metrics.Tag.SERVICE_RUNNABLE)
    );
  }

  private static List<Aggregation> createAggregations() {
    List<Aggregation> aggs = Lists.newLinkedList();

    // <cluster metrics>, e.g. storage used
    aggs.add(new DefaultAggregation(ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.CLUSTER_METRICS)));

    // app, prg type, prg name, ...
    aggs.add(new DefaultAggregation(ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
      // todo: do we even need program type? seems like program name unique within app across program types
      Constants.Metrics.Tag.PROGRAM_TYPE, Constants.Metrics.Tag.PROGRAM, Constants.Metrics.Tag.RUN_ID,
      PROGRAM_LEVEL2, PROGRAM_LEVEL3, PROGRAM_LEVEL4, Constants.Metrics.Tag.DATASET),
                                    // i.e. for programs only
                                    ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
      Constants.Metrics.Tag.PROGRAM_TYPE, Constants.Metrics.Tag.PROGRAM)));

    // component, handler, method
    aggs.add(new DefaultAggregation(ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.COMPONENT,
      Constants.Metrics.Tag.HANDLER, Constants.Metrics.Tag.METHOD),
                                    // i.e. for components only
                                    ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.COMPONENT)));

    // component, handler, method, stream (for stream only) todo: seems like emitted context is wrong, review...
    aggs.add(new DefaultAggregation(ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE,
      Constants.Metrics.Tag.COMPONENT, Constants.Metrics.Tag.HANDLER, Constants.Metrics.Tag.METHOD,
      Constants.Metrics.Tag.STREAM),
                                    // i.e. for stream only
                                    ImmutableList.of(Constants.Metrics.Tag.STREAM)));

    // dataset
    aggs.add(new DefaultAggregation(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.DATASET),
                                    // i.e. for datasets only
                                    ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.DATASET)));

    return aggs;
  }

  @Override
  public void add(MetricValue metricValue) throws Exception {
    String scope = metricValue.getTags().get(Constants.Metrics.Tag.SCOPE);
    String measureName = (scope == null ? "system." : scope + ".") + metricValue.getName();

    CubeFact fact = new CubeFact(replaceTagsIfNeeded(metricValue.getTags()),
                                 toMeasureType(metricValue.getType()), measureName,
                                 new TimeValue(metricValue.getTimestamp(), metricValue.getValue()));
    cube.get().add(fact);
  }

  @Override
  public Collection<MetricTimeSeries> query(MetricDataQuery q) throws Exception {
    Collection<TimeSeries> cubeResult = cube.get().query(buildCubeQuery(q));
    List<MetricTimeSeries> result = Lists.newArrayList();
    for (TimeSeries timeSeries : cubeResult) {

      result.add(new MetricTimeSeries(timeSeries.getMeasureName(),
                                      unmapTags(timeSeries.getTagValues(),
                                                q.getSliceByTags().get(Constants.Metrics.Tag.PROGRAM_TYPE)),
                                      timeSeries.getTimeValues()));
    }
    return result;
  }

  private CubeQuery buildCubeQuery(MetricDataQuery q) {
    return new CubeQuery(q.getStartTs(), q.getEndTs(), q.getResolution(), q.getMetricName(),
                                          toMeasureType(q.getMetricType()), replaceTagsIfNeeded(q.getSliceByTags()),
                                          replaceTagsIfNeeded(q.getGroupByTags()));
  }

  private Map<String, String> unmapTags(Map<String, String> tagValues, @Nullable String programType) {
    if (programType == null) {
      return tagValues;
    }
    Map<String, String> result = Maps.newHashMap();
    for (Map.Entry<String, String> tagValue : tagValues.entrySet()) {
      String tagNameReplacement = reverseMapping.get(programType).get(tagValue.getKey());
      result.put(tagNameReplacement == null ? tagValue.getKey() : tagNameReplacement, tagValue.getValue());
    }
    return result;
  }

  @Override
  public void deleteBefore(long timestamp) throws Exception {
    // delete all data before the timestamp. null for MeasureName indicates match any MeasureName.
    // note: We are using 1 as start ts, so that we do not delete data from "totals". This method is applied in
    //       in-memory and standalone modes, so it is fine to keep totals in these cases during TTL
    //       todo: Cube and FactTable must use resolution when applying time range conditions
    CubeDeleteQuery query = new CubeDeleteQuery(1, timestamp, null, Maps.<String, String>newHashMap());
    cube.get().delete(query);
  }

  @Override
  public void delete(MetricDeleteQuery query) throws Exception {
    cube.get().delete(buildCubeDeleteQuery(query));
  }

  private CubeDeleteQuery buildCubeDeleteQuery(MetricDeleteQuery query) {
    Map<String, String> sliceByTagValues = replaceTagsIfNeeded(query.getSliceByTags());
    return new CubeDeleteQuery(query.getStartTs(), query.getEndTs(),
                                                           query.getMetricName(), sliceByTagValues);
  }

  private List<TagValue> replaceTagValuesIfNeeded(List<TagValue> tagValues) {
    List<TagValue> result = Lists.newArrayList();
    for (TagValue tagValue : tagValues) {
      String tagNameReplacement = tagMapping.get(tagValue.getTagName());
      result.add(new TagValue(tagNameReplacement == null ? tagValue.getTagName() : tagNameReplacement,
                              tagValue.getValue()));
    }
    return result;
  }

  @Override
  public Collection<TagValue> findNextAvailableTags(MetricSearchQuery query) throws Exception {
    Collection<TagValue> tagsFromCube = cube.get().findNextAvailableTags(buildCubeSearchQuery(query));
    String programType = getTagValue(query.getTagValues(), Constants.Metrics.Tag.PROGRAM_TYPE);
    return unmapTags(tagsFromCube, programType);
  }

  private CubeExploreQuery buildCubeSearchQuery(MetricSearchQuery query) {
    return new CubeExploreQuery(query.getStartTs(), query.getEndTs(), query.getResolution(), query.getLimit(),
                                                                 replaceTagValuesIfNeeded(query.getTagValues()));
  }

  @Override
  public Collection<String> findMetricNames(MetricSearchQuery query) throws Exception {
    return cube.get().findMeasureNames(buildCubeSearchQuery(query));
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

  private Collection<TagValue> unmapTags(Collection<TagValue> nextAvailableTags, @Nullable String programType) {
    // check if program-type is available in the query, if available , check if reverseMapping contains
    // value for the next tag key. if so, replace the tag-key , else use existing tag-key
    if (programType == null || (reverseMapping.get(programType) == null)) {
      return nextAvailableTags;
    }

    List<TagValue> unMappedList = Lists.newArrayList();
    for (TagValue tagValue : nextAvailableTags) {
      String tag = reverseMapping.get(programType).get(tagValue.getTagName());
      unMappedList.add(tag == null ? tagValue : new TagValue(tag, tagValue.getValue()));
    }
    return unMappedList;
  }

  @Nullable
  private String getTagValue(List<TagValue> tagValues, String tagName) {
    String programType = null;
    for (TagValue tagValue : tagValues) {
      if (tagValue.getTagName().equals(tagName)) {
        programType = tagValue.getValue();
        break;
      }
    }
    return programType;
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
