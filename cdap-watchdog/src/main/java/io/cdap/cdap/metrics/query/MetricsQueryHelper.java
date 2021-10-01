/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.metrics.query;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.AggregationOption;
import io.cdap.cdap.api.dataset.lib.cube.Interpolator;
import io.cdap.cdap.api.dataset.lib.cube.Interpolators;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricSearchQuery;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.metrics.TagValue;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.TimeMathParser;
import io.cdap.cdap.proto.MetricQueryRequest;
import io.cdap.cdap.proto.MetricQueryResult;
import io.cdap.cdap.proto.MetricTagValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 *
 */
public class MetricsQueryHelper {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsQueryHelper.class);

  public static final String NAMESPACE_STRING = "namespace";
  public static final String APP_STRING = "app";

  // constants used for request query parsing
  private static final String PARAM_COUNT = "count";
  private static final String PARAM_START_TIME = "start";
  private static final String PARAM_RESOLUTION = "resolution";
  private static final String PARAM_END_TIME = "end";
  private static final String PARAM_INTERPOLATE = "interpolate";
  private static final String PARAM_STEP_INTERPOLATOR = "step";
  private static final String PARAM_LINEAR_INTERPOLATOR = "linear";
  private static final String PARAM_MAX_INTERPOLATE_GAP = "maxInterpolateGap";
  private static final String PARAM_AGGREGATE = "aggregate";
  private static final String PARAM_AUTO_RESOLUTION = "auto";
  private static final String ANY_TAG_VALUE = "*";

  private final MetricStore metricStore;
  private final int minResolution;

  private static final Map<String, String> tagNameToHuman;
  private static final Map<String, String> humanToTagName;

  static {
    ImmutableBiMap<String, String> mapping = ImmutableBiMap.<String, String>builder()
      .put(Constants.Metrics.Tag.NAMESPACE, NAMESPACE_STRING)
      .put(Constants.Metrics.Tag.RUN_ID, "run")
      .put(Constants.Metrics.Tag.INSTANCE_ID, "instance")

      .put(Constants.Metrics.Tag.COMPONENT, "component")
      .put(Constants.Metrics.Tag.HANDLER, "handler")
      .put(Constants.Metrics.Tag.METHOD, "method")

      .put(Constants.Metrics.Tag.DATASET, "dataset")

      .put(Constants.Metrics.Tag.APP, APP_STRING)

      .put(Constants.Metrics.Tag.SERVICE, "service")
      // SERVICE_HANDLER is the same HANDLER

      .put(Constants.Metrics.Tag.WORKER, "worker")

      .put(Constants.Metrics.Tag.PRODUCER, "producer")
      .put(Constants.Metrics.Tag.CONSUMER, "consumer")

      .put(Constants.Metrics.Tag.MAPREDUCE, "mapreduce")
      .put(Constants.Metrics.Tag.MR_TASK_TYPE, "tasktype")

      .put(Constants.Metrics.Tag.WORKFLOW, "workflow")

      .put(Constants.Metrics.Tag.PROVISIONER, "provisioner")

      .put(Constants.Metrics.Tag.SPARK, "spark")
      .put(Constants.Metrics.Tag.STATUS, "status")

      // put program related tag
      .put(Constants.Metrics.Tag.PROGRAM, "program")
      .put(Constants.Metrics.Tag.PROGRAM_TYPE, "programtype")

      // put profile related tag
      .put(Constants.Metrics.Tag.PROFILE, "profile")
      .put(Constants.Metrics.Tag.PROFILE_SCOPE, "profilescope")
      .put(Constants.Metrics.Tag.CLASS, "class")
      .put(Constants.Metrics.Tag.TRIES, "retry")
      .build();

    tagNameToHuman = mapping;
    humanToTagName = mapping.inverse();
  }

  @Inject
  public MetricsQueryHelper(MetricStore metricStore, CConfiguration cConf) {
    this.metricStore = metricStore;
    int minimumResolution = cConf.getInt(Constants.Metrics.METRICS_MINIMUM_RESOLUTION_SECONDS);
    this.minResolution = minimumResolution < 60 ? minimumResolution : 60;
  }

  public List<MetricTagValue> searchTags(List<String> tags) {
    // we want to search the entire range, so startTimestamp is '0' and end Timestamp is Integer.MAX_VALUE and
    // limit is -1 , to include the entire search result.
    MetricSearchQuery searchQuery = new MetricSearchQuery(0, Integer.MAX_VALUE, -1,
                                                          toTagValues(humanToTagNames(parseTagValues(tags))));
    return tagValuesToHuman(metricStore.findNextAvailableTags(searchQuery));
  }

  public Collection<String> searchMetric(List<String> tagValues) {
    return getMetrics(humanToTagNames(parseTagValues(tagValues)));
  }

  public Map<String, MetricQueryResult> executeBatchQueries(Map<String, QueryRequestFormat> queries) throws Exception {
    LOG.trace("Received Queries {}", queries);
    Map<String, MetricQueryResult> queryFinalResponse = Maps.newHashMap();
    for (Map.Entry<String, QueryRequestFormat> query : queries.entrySet()) {
      MetricQueryRequest queryRequest = getQueryRequestFromFormat(query.getValue());
      queryFinalResponse.put(query.getKey(), executeQuery(queryRequest));
    }
    return queryFinalResponse;
  }

  public MetricQueryResult executeTagQuery(List<String> tags, List<String> metrics, List<String> groupByTags,
                                           Map<String, List<String>> queryTimeParams) throws Exception {
    MetricQueryRequest queryRequest = new MetricQueryRequest(parseTagValuesAsMap(tags), metrics, groupByTags);
    setTimeRangeInQueryRequest(queryRequest, queryTimeParams);
    return executeQuery(queryRequest);
  }

  @VisibleForTesting
  public MetricStore getMetricStore() {
    return metricStore;
  }

  private Collection<String> getMetrics(List<MetricTagValue> tagValues) {
    // we want to search the entire range, so startTimestamp is '0' and end Timestamp is Integer.MAX_VALUE and
    // limit is -1 , to include the entire search result.
    MetricSearchQuery searchQuery =
      new MetricSearchQuery(0, Integer.MAX_VALUE, -1, toTagValues(tagValues));
    Collection<String> metricNames = metricStore.findMetricNames(searchQuery);
    return Lists.newArrayList(Iterables.filter(metricNames, Predicates.notNull()));
  }

  private List<TagValue> toTagValues(List<MetricTagValue> tagValues) {
    return Lists.transform(tagValues, new Function<MetricTagValue, TagValue>() {
      @Nullable
      @Override
      public TagValue apply(@Nullable MetricTagValue input) {
        if (input == null) {
          // SHOULD NEVER happen
          throw new NullPointerException();
        }
        return new TagValue(input.getName(), input.getValue());
      }
    });
  }

  private List<MetricTagValue> humanToTagNames(List<MetricTagValue> tagValues) {
    List<MetricTagValue> result = Lists.newArrayList();
    for (MetricTagValue tagValue : tagValues) {
      String tagName = humanToTagName(tagValue.getName());
      result.add(new MetricTagValue(tagName, tagValue.getValue()));
    }
    return result;
  }

  private List<MetricTagValue> parseTagValues(List<String> tags) {
    List<MetricTagValue> result = Lists.newArrayList();
    for (String tag : tags) {
      // split by ':' and add the tagValue to result list
      String[] tagSplit = tag.split(":", 2);
      if (tagSplit.length == 2) {
        String value = tagSplit[1].equals(ANY_TAG_VALUE) ? null : tagSplit[1];
        result.add(new MetricTagValue(tagSplit[0], value));
      }
    }
    return result;
  }

  private String humanToTagName(String humanTagName) {
    String replacement = humanToTagName.get(humanTagName);
    return replacement != null ? replacement : humanTagName;
  }

  private List<MetricTagValue> tagValuesToHuman(Collection<TagValue> tagValues) {
    List<MetricTagValue> result = Lists.newArrayList();
    for (TagValue tagValue : tagValues) {
      String human = tagNameToHuman.get(tagValue.getName());
      human = human != null ? human : tagValue.getName();
      String value = tagValue.getValue() == null ? ANY_TAG_VALUE : tagValue.getValue();
      result.add(new MetricTagValue(human, value));
    }
    return result;
  }

  private MetricQueryRequest getQueryRequestFromFormat(QueryRequestFormat queryRequestFormat) {
    Map<String, List<String>> queryParams = Maps.newHashMap();

    for (Map.Entry<String, String> entry : queryRequestFormat.getTimeRange().entrySet()) {
      queryParams.put(entry.getKey(), ImmutableList.of(entry.getValue()));
    }

    MetricQueryRequest queryRequest = new MetricQueryRequest(queryRequestFormat.getTags(),
                                                             queryRequestFormat.getMetrics(),
                                                             queryRequestFormat.getGroupBy());
    setTimeRangeInQueryRequest(queryRequest, queryParams);
    return queryRequest;
  }

  private void setTimeRangeInQueryRequest(MetricQueryRequest request, Map<String, List<String>> queryTimeParams) {
    Long start =
      queryTimeParams.containsKey(PARAM_START_TIME) ?
        TimeMathParser.parseTimeInSeconds(queryTimeParams.get(PARAM_START_TIME).get(0)) : null;
    Long end =
      queryTimeParams.containsKey(PARAM_END_TIME) ?
        TimeMathParser.parseTimeInSeconds(queryTimeParams.get(PARAM_END_TIME).get(0)) : null;
    Integer count = null;

    AggregationOption aggregationOption =
      queryTimeParams.containsKey(PARAM_AGGREGATE)
        ? AggregationOption.valueOf(queryTimeParams.get(PARAM_AGGREGATE).get(0).toUpperCase())
        : AggregationOption.FALSE;
    boolean aggregate = aggregationOption.equals(AggregationOption.TRUE) || ((start == null) && (end == null));

    Integer resolution = queryTimeParams.containsKey(PARAM_RESOLUTION) ?
      getResolution(queryTimeParams.get(PARAM_RESOLUTION).get(0), start, end) : getResolution(null, start, end);

    Interpolator interpolator = null;
    if (queryTimeParams.containsKey(PARAM_INTERPOLATE)) {
      long timeLimit = queryTimeParams.containsKey(PARAM_MAX_INTERPOLATE_GAP) ?
        Long.parseLong(queryTimeParams.get(PARAM_MAX_INTERPOLATE_GAP).get(0)) : Long.MAX_VALUE;
      interpolator = getInterpolator(queryTimeParams.get(PARAM_INTERPOLATE).get(0), timeLimit);
    }

    if (queryTimeParams.containsKey(PARAM_COUNT)) {
      count = Integer.valueOf(queryTimeParams.get(PARAM_COUNT).get(0));
      if (start == null && end != null) {
        start = end - count * resolution;
      } else if (start != null && end == null) {
        end = start + count * resolution;
      }
    } else if (start != null && end != null) {
      count = (int) (((end / resolution * resolution) - (start / resolution * resolution)) / resolution + 1);
    } else if (!aggregate) {
      throw new IllegalArgumentException("At least two of count/start/end parameters " +
                                           "are required for time-range queries ");
    }

    if (aggregate) {
      request.setTimeRange(0L, 0L, 1, Integer.MAX_VALUE, null, aggregationOption);
    } else {
      request.setTimeRange(start, end, count, resolution, interpolator, aggregationOption);
    }
  }

  /**
   * Get the integer resolution based on the resolution string, start and end ts. The logic of determining resolution
   * is:
   * 1. If the resolution string is a specific time interval, for example, 1s, 1m, 60s, and if this interval exists in
   * the available resolution, that resolution will get returned.
   * 2. If the resolution "auto", then start and end timestamp must be specified to determine the correct resolution.
   * If end - start > 10 hours, hour resolution will be used. If end - start > 10 mins, min resolution will be used.
   * If end -start < 10 mins, minimum resolution will be used.
   * 3. If the resolution is null, i.e, not specified in the query, then if start and end timestamp are not null,
   * the logic will be same as the resolution is "auto". If any of the start or end timestamp is not specified,
   * minimum resolution will be used.
   *
   * @param resolution the resolution string, can be specific resolution like 1s, 1m, etc, or can be auto or null.
   * @param start the start timestamp, null if not specified in the query
   * @param end the end timestamp, null if not specified in the query
   * @return the integer resolution for the query
   */
  @VisibleForTesting
  Integer getResolution(@Nullable String resolution, @Nullable Long start, @Nullable Long end) {
    if (resolution == null || resolution.equals(PARAM_AUTO_RESOLUTION)) {
      if (start != null && end != null) {
        long difference = end - start;
        if (difference < 0) {
          throw new IllegalArgumentException(String.format("The start time %d should not be " +
                                                             "larger than the end time %d", start, end));
        }
        return getResolution(difference);
      } else if (resolution != null) {
        throw new IllegalArgumentException("if resolution=auto, start and end timestamp " +
                                             "should be provided to determine resolution");
      } else {
        return minResolution;
      }
    } else {
      // if not auto, check if the given resolution matches available resolutions that we support.
      int resolutionInterval = TimeMathParser.resolutionInSeconds(resolution);
      if (!((resolutionInterval == Integer.MAX_VALUE) || (resolutionInterval == 3600) ||
        (resolutionInterval == 60) || (resolutionInterval == minResolution))) {
        throw new IllegalArgumentException(String.format("Resolution interval not supported, only %d second, " +
                                                           "1 minute and 1 hour resolutions are supported currently",
                                                         minResolution));
      }
      return resolutionInterval;
    }
  }

  private int getResolution(long difference) {
    if (difference > Constants.Metrics.Query.MAX_HOUR_RESOLUTION_QUERY_INTERVAL) {
      return 3600;
    } else if (difference > Constants.Metrics.Query.MAX_MINUTE_RESOLUTION_QUERY_INTERVAL) {
      return 60;
    } else {
      return minResolution;
    }
  }

  private Interpolator getInterpolator(String interpolator, long timeLimit) {
    if (PARAM_STEP_INTERPOLATOR.equals(interpolator)) {
      return new Interpolators.Step(timeLimit);
    } else if (PARAM_LINEAR_INTERPOLATOR.equals(interpolator)) {
      return new Interpolators.Linear(timeLimit);
    }
    return null;
  }

  private MetricQueryResult executeQuery(MetricQueryRequest queryRequest) throws Exception {
    if (queryRequest.getMetrics().size() == 0) {
      throw new IllegalArgumentException("Missing metrics parameter in the query");
    }

    MetricQueryRequest.TimeRange timeRange = queryRequest.getTimeRange();
    AggregationOption aggregation = timeRange.getAggregation();
    if (timeRange.getCount() <= 0) {
      throw new IllegalArgumentException("Invalid metrics aggregation request, the limit must be greater than 0");
    }

    Map<String, String> tagsSliceBy = humanToTagNames(transformTagMap(queryRequest.getTags()));

    MetricDataQuery query = new MetricDataQuery(timeRange.getStart(), timeRange.getEnd(),
                                                timeRange.getResolutionInSeconds(),
                                                timeRange.getCount(), toMetrics(queryRequest.getMetrics()),
                                                tagsSliceBy, transformGroupByTags(queryRequest.getGroupBy()),
                                                aggregation, timeRange.getInterpolate());
    Collection<MetricTimeSeries> queryResult = metricStore.query(query);

    long endTime = timeRange.getEnd();
    if (timeRange.getResolutionInSeconds() == Integer.MAX_VALUE && endTime == 0) {
      // for aggregate query, we set the end time to be query time (current time)
      endTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    }

    return decorate(queryResult, timeRange.getStart(), endTime, timeRange.getResolutionInSeconds());
  }

  private Map<String, String> transformTagMap(Map<String, String> tags) {
    return Maps.transformValues(tags, new Function<String, String>() {
      @Override
      public String apply(String value) {
        if (ANY_TAG_VALUE.equals(value)) {
          return null;
        } else {
          return value;
        }
      }
    });
  }

  private List<String> transformGroupByTags(List<String> groupBy) {
    return Lists.transform(groupBy, new Function<String, String>() {
      @Nullable
      @Override
      public String apply(@Nullable String input) {
        String replacement = humanToTagName.get(input);
        return replacement != null ? replacement : input;
      }
    });
  }

  private Map<String, String> parseTagValuesAsMap(List<String> tags) {
    List<MetricTagValue> tagValues = parseTagValues(tags);

    Map<String, String> result = Maps.newHashMap();
    for (MetricTagValue tagValue : tagValues) {
      result.put(tagValue.getName(), tagValue.getValue());
    }
    return result;
  }

  private Map<String, String> humanToTagNames(Map<String, String> tagValues) {
    Map<String, String> result = Maps.newHashMap();
    for (Map.Entry<String, String> tagValue : tagValues.entrySet()) {
      result.put(humanToTagName(tagValue.getKey()), tagValue.getValue());
    }
    return result;
  }

  private Map<String, AggregationFunction> toMetrics(List<String> metrics) {
    Map<String, AggregationFunction> result = Maps.newHashMap();
    for (String metric : metrics) {
      // todo: figure out metric type
      result.put(metric, AggregationFunction.SUM);
    }
    return result;
  }

  private MetricQueryResult decorate(Collection<MetricTimeSeries> series, long startTs, long endTs,
                                     int resolution) {
    MetricQueryResult.TimeSeries[] serieses = new MetricQueryResult.TimeSeries[series.size()];
    int i = 0;
    for (MetricTimeSeries timeSeries : series) {
      MetricQueryResult.TimeValue[] timeValues = decorate(timeSeries.getTimeValues());
      serieses[i++] = new MetricQueryResult.TimeSeries(timeSeries.getMetricName(),
                                                       tagNamesToHuman(timeSeries.getTagValues()), timeValues);
    }
    return new MetricQueryResult(startTs, endTs, serieses, resolution);
  }

  private MetricQueryResult.TimeValue[] decorate(List<TimeValue> points) {
    MetricQueryResult.TimeValue[] timeValues = new MetricQueryResult.TimeValue[points.size()];
    int k = 0;
    for (TimeValue timeValue : points) {
      timeValues[k++] = new MetricQueryResult.TimeValue(timeValue.getTimestamp(), timeValue.getValue());
    }
    return timeValues;
  }

  private Map<String, String> tagNamesToHuman(Map<String, String> tagValues) {
    Map<String, String> humanTagValues = Maps.newHashMap();
    for (Map.Entry<String, String> tag : tagValues.entrySet()) {
      humanTagValues.put(tagNameToHuman.get(tag.getKey()), tag.getValue());
    }
    return humanTagValues;
  }

  /**
   * Helper class to Deserialize Query requests and based on this
   * {@link MetricQueryRequest} will be constructed
   */
  public class QueryRequestFormat {
    private Map<String, String> tags;
    private List<String> metrics;
    private List<String> groupBy;
    private Map<String, String> timeRange;

    public Map<String, String> getTags() {
      tags = tags == null ? Collections.emptyMap() : tags;
      return tags;
    }

    public List<String> getMetrics() {
      metrics = metrics == null ? Collections.emptyList() : metrics;
      return metrics;
    }

    public List<String> getGroupBy() {
      groupBy = groupBy == null ? Collections.emptyList() : groupBy;
      return groupBy;
    }

    /**
     * time range has aggregate=true or {start, end, count, resolution, interpolate} parameters,
     * since start, end can be represented as 'now ('+' or '-')' and not just absolute timestamp,
     * we use this format to get those strings and after parsing and determining other parameters, we can construct
     * {@link MetricQueryRequest} , similar for resolution.
     * @return time range prameters
     */
    public Map<String, String> getTimeRange() {
      timeRange = (timeRange == null || timeRange.size() == 0) ? ImmutableMap.of("aggregate", "true") : timeRange;
      return timeRange;
    }
  }
}
