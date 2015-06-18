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

package co.cask.cdap.metrics.query;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.Interpolator;
import co.cask.cdap.api.dataset.lib.cube.Interpolators;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.proto.MetricQueryRequest;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.MetricTagValue;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Search metrics handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/metrics")
public class MetricsHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsHandler.class);
  private static final Gson GSON = new Gson();

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

  public static final String ANY_TAG_VALUE = "*";

  private final MetricStore metricStore;

  private static final Map<String, String> tagNameToHuman;
  private static final Map<String, String> humanToTagName;

  static {
    ImmutableBiMap<String, String> mapping = ImmutableBiMap.<String, String>builder()
      .put(Constants.Metrics.Tag.NAMESPACE, "namespace")
      .put(Constants.Metrics.Tag.RUN_ID, "run")
      .put(Constants.Metrics.Tag.INSTANCE_ID, "instance")

      .put(Constants.Metrics.Tag.COMPONENT, "component")
      .put(Constants.Metrics.Tag.HANDLER, "handler")
      .put(Constants.Metrics.Tag.METHOD, "method")

      .put(Constants.Metrics.Tag.STREAM, "stream")

      .put(Constants.Metrics.Tag.DATASET, "dataset")

      .put(Constants.Metrics.Tag.APP, "app")
      .put(Constants.Metrics.Tag.ADAPTER, "adapter")

      .put(Constants.Metrics.Tag.SERVICE, "service")
      // SERVICE_HANDLER is the same HANDLER

      .put(Constants.Metrics.Tag.WORKER, "worker")

      .put(Constants.Metrics.Tag.FLOW, "flow")
      .put(Constants.Metrics.Tag.FLOWLET, "flowlet")
      .put(Constants.Metrics.Tag.FLOWLET_QUEUE, "queue")

      .put(Constants.Metrics.Tag.PRODUCER, "producer")
      .put(Constants.Metrics.Tag.CONSUMER, "consumer")

      .put(Constants.Metrics.Tag.MAPREDUCE, "mapreduce")
      .put(Constants.Metrics.Tag.MR_TASK_TYPE, "tasktype")

      .put(Constants.Metrics.Tag.WORKFLOW, "workflow")

      .put(Constants.Metrics.Tag.SPARK, "spark").build();

    tagNameToHuman = mapping;
    humanToTagName = mapping.inverse();
  }

  @Inject
  public MetricsHandler(MetricStore metricStore) {
    this.metricStore = metricStore;
  }

  @POST
  @Path("/search")
  public void search(HttpRequest request, HttpResponder responder,
                     @QueryParam("target") String target,
                     @QueryParam("tag") List<String> tags) throws IOException {
    if (target == null) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Required target param is missing");
      return;
    }

    switch (target) {
      case "tag":
        searchTagAndRespond(responder, tags);
        break;
      case "metric":
        searchMetricAndRespond(responder, tags);
        break;
      default:
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Unknown target param value: " + target);
        break;
    }
  }

  private void searchMetricAndRespond(HttpResponder responder, List<String> tagValues) {
    try {
      responder.sendJson(HttpResponseStatus.OK, getMetrics(humanToTagNames(parseTagValues(tagValues))));
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.warn("Exception while retrieving available metrics", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
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

  @POST
  @Path("/query")
  public void query(HttpRequest request, HttpResponder responder,
                    @QueryParam("metric") List<String> metrics,
                    @QueryParam("groupBy") List<String> groupBy,
                    @QueryParam("tag") List<String> tags) throws Exception {

    if (new QueryStringDecoder(request.getUri()).getParameters().isEmpty()) {
      executeBatchQueries(request, responder);
      return;
    }

    tagsQuerying(request, responder, tags, metrics, groupBy);
  }

  private void executeBatchQueries(HttpRequest request, HttpResponder responder) {
    if (HttpHeaders.getContentLength(request) > 0) {
      try {
        String json = request.getContent().toString(Charsets.UTF_8);
        Map<String, QueryRequestFormat> queries =
          GSON.fromJson(json, new TypeToken<Map<String, QueryRequestFormat>>() { }.getType());

        LOG.trace("Received Queries {}", queries);

        Map<String, MetricQueryResult> queryFinalResponse = Maps.newHashMap();
        for (Map.Entry<String, QueryRequestFormat> query : queries.entrySet()) {
          MetricQueryRequest queryRequest = getQueryRequestFromFormat(query.getValue());
          queryFinalResponse.put(query.getKey(), executeQuery(queryRequest));
        }
        responder.sendJson(HttpResponseStatus.OK, queryFinalResponse);
      } catch (IllegalArgumentException e) {
        LOG.warn("Invalid request", e);
        responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      } catch (Exception e) {
        LOG.error("Exception querying metrics ", e);
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
      }
    } else {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Batch request with empty content");
    }
  }

  private MetricQueryRequest getQueryRequestFromFormat(QueryRequestFormat queryRequestFormat) {
    Map<String, List<String>> queryParams = Maps.newHashMap();

    for (Map.Entry<String, String> entry : queryRequestFormat.getTimeRange().entrySet()) {
      queryParams.put(entry.getKey(), ImmutableList.of(entry.getValue()));
    }

    MetricQueryRequest queryRequest = new MetricQueryRequest(queryRequestFormat.getTags(),
                                                 queryRequestFormat.getMetrics(), queryRequestFormat.getGroupBy());
    setTimeRangeInQueryRequest(queryRequest, queryParams);
    return queryRequest;
  }

  private void tagsQuerying(HttpRequest request, HttpResponder responder, List<String> tags, List<String> metrics,
                            List<String> groupByTags) {
    try {
      responder.sendJson(HttpResponseStatus.OK, executeQuery(request, parseTagValuesAsMap(tags), groupByTags, metrics));
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    }
  }

  private MetricQueryResult executeQuery(HttpRequest request, Map<String, String> sliceByTags,
                                         List<String> groupByTags, List<String> metrics) throws Exception {
    MetricQueryRequest queryRequest = new MetricQueryRequest(sliceByTags, metrics, groupByTags);
    setTimeRangeInQueryRequest(queryRequest, new QueryStringDecoder(request.getUri()).getParameters());
    return executeQuery(queryRequest);
  }

  private void setTimeRangeInQueryRequest(MetricQueryRequest request, Map<String, List<String>> queryTimeParams) {
    Long start =
      queryTimeParams.containsKey(PARAM_START_TIME) ?
        TimeMathParser.parseTimeInSeconds(queryTimeParams.get(PARAM_START_TIME).get(0)) : null;
    Long end =
      queryTimeParams.containsKey(PARAM_END_TIME) ?
        TimeMathParser.parseTimeInSeconds(queryTimeParams.get(PARAM_END_TIME).get(0)) : null;
    Integer count = null;

    boolean aggregate =
      queryTimeParams.containsKey(PARAM_AGGREGATE) && queryTimeParams.get(PARAM_AGGREGATE).get(0).equals("true") ||
        ((start == null) && (end == null));

    Integer resolution = queryTimeParams.containsKey(PARAM_RESOLUTION) ?
      getResolution(queryTimeParams.get(PARAM_RESOLUTION).get(0), start, end) : 1;

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
      request.setTimeRange(0L, 0L, 1, Integer.MAX_VALUE, null);
    } else {
      request.setTimeRange(start, end, count, resolution, interpolator);
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

  private Integer getResolution(String resolution, Long start, Long end) {
    if (resolution.equals(PARAM_AUTO_RESOLUTION)) {
      if (start != null && end != null) {
        long difference = end - start;
        return MetricQueryParser.getResolution(difference).getResolution();
      } else {
        throw new IllegalArgumentException("if resolution=auto, start and end timestamp " +
                                             "should be provided to determine resolution");
      }
    } else {
      // if not auto, check if the given resolution matches available resolutions that we support.
      int resolutionInterval = TimeMathParser.resolutionInSeconds(resolution);
      if (!((resolutionInterval == Integer.MAX_VALUE) || (resolutionInterval == 3600) ||
        (resolutionInterval == 60) || (resolutionInterval == 1))) {
        throw new IllegalArgumentException("Resolution interval not supported, only 1 second, 1 minute and " +
                                             "1 hour resolutions are supported currently");
      }
      return resolutionInterval;
    }
  }

  private MetricQueryResult executeQuery(MetricQueryRequest queryRequest) throws Exception {
    if (queryRequest.getMetrics().size() == 0) {
      throw new IllegalArgumentException("Missing metrics parameter in the query");
    }

    Map<String, String> tagsSliceBy = humanToTagNames(transformTagMap(queryRequest.getTags()));

    MetricQueryRequest.TimeRange timeRange = queryRequest.getTimeRange();

    MetricDataQuery query = new MetricDataQuery(timeRange.getStart(), timeRange.getEnd(),
                                                timeRange.getResolutionInSeconds(),
                                                timeRange.getCount(), toMetrics(queryRequest.getMetrics()),
                                                tagsSliceBy, transformGroupByTags(queryRequest.getGroupBy()),
                                                timeRange.getInterpolate());
    Collection<MetricTimeSeries> queryResult = metricStore.query(query);

    long endTime = timeRange.getEnd();
    if (timeRange.getResolutionInSeconds() == Integer.MAX_VALUE && endTime == 0) {
      // for aggregate query, we set the end time to be query time (current time)
      endTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    }

    return decorate(queryResult, timeRange.getStart(), endTime, timeRange.getResolutionInSeconds());
  }

  private Map<String, AggregationFunction> toMetrics(List<String> metrics) {
    Map<String, AggregationFunction> result = Maps.newHashMap();
    for (String metric : metrics) {
      // todo: figure out metric type
      result.put(metric, AggregationFunction.SUM);
    }
    return result;
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

  private void searchTagAndRespond(HttpResponder responder, List<String> tags) {
    try {
      // we want to search the entire range, so startTimestamp is '0' and end Timestamp is Integer.MAX_VALUE and
      // limit is -1 , to include the entire search result.
      MetricSearchQuery searchQuery = new MetricSearchQuery(0, Integer.MAX_VALUE, -1,
                                                            toTagValues(humanToTagNames(parseTagValues(tags))));
      Collection<TagValue> nextTags = metricStore.findNextAvailableTags(searchQuery);
      responder.sendJson(HttpResponseStatus.OK, tagValuesToHuman(nextTags));
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    }
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

  private String humanToTagName(String humanTagName) {
    String replacement = humanToTagName.get(humanTagName);
    return replacement != null ? replacement : humanTagName;
  }

  private Map<String, String> humanToTagNames(Map<String, String> tagValues) {
    Map<String, String> result = Maps.newHashMap();
    for (Map.Entry<String, String> tagValue : tagValues.entrySet()) {
      result.put(humanToTagName(tagValue.getKey()), tagValue.getValue());
    }
    return result;
  }

  private Collection<String> getMetrics(List<MetricTagValue> tagValues) throws Exception {
    // we want to search the entire range, so startTimestamp is '0' and end Timestamp is Integer.MAX_VALUE and
    // limit is -1 , to include the entire search result.
    MetricSearchQuery searchQuery =
      new MetricSearchQuery(0, Integer.MAX_VALUE, -1, toTagValues(tagValues));
    Collection<String> metricNames = metricStore.findMetricNames(searchQuery);
    return Lists.newArrayList(Iterables.filter(metricNames, Predicates.notNull()));
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

  private Map<String, String> tagNamesToHuman(Map<String, String> tagValues) {
    Map<String, String> humanTagValues = Maps.newHashMap();
    for (Map.Entry<String, String> tag : tagValues.entrySet()) {
      humanTagValues.put(tagNameToHuman.get(tag.getKey()), tag.getValue());
    }
    return humanTagValues;
  }

  private MetricQueryResult.TimeValue[] decorate(List<TimeValue> points) {
    MetricQueryResult.TimeValue[] timeValues = new MetricQueryResult.TimeValue[points.size()];
    int k = 0;
    for (TimeValue timeValue : points) {
      timeValues[k++] = new MetricQueryResult.TimeValue(timeValue.getTimestamp(), timeValue.getValue());
    }
    return timeValues;
  }

  /**
   * Helper class to Deserialize Query requests and based on this
   * {@link MetricQueryRequest} will be constructed
   */
  private class QueryRequestFormat {
    Map<String, String> tags;
    List<String> metrics;
    List<String> groupBy;
    Map<String, String> timeRange;

    public Map<String, String> getTags() {
      tags = (tags == null) ? Maps.<String, String>newHashMap() : tags;
      return tags;
    }

    public List<String> getMetrics() {
      return metrics;
    }

    public List<String> getGroupBy() {
      groupBy = (groupBy == null) ? Lists.<String>newArrayList() : groupBy;
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
