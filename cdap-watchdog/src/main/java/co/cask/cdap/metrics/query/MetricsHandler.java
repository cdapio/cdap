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

import co.cask.cdap.api.dataset.lib.cube.Interpolators;
import co.cask.cdap.api.dataset.lib.cube.TagValue;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.QueryRequest;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Search metrics handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/metrics")
public class MetricsHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsDiscoveryHandler.class);
  private static final Gson GSON = new Gson();

  // constants used for request query parsing
  private static final String COUNT = "count";
  private static final String START_TIME = "start";
  private static final String RESOLUTION = "resolution";
  private static final String END_TIME = "end";
  private static final String INTERPOLATE = "interpolate";
  private static final String STEP_INTERPOLATOR = "step";
  private static final String LINEAR_INTERPOLATOR = "linear";
  private static final String MAX_INTERPOLATE_GAP = "maxInterpolateGap";
  private static final String AGGREGATE = "aggregate";
  private static final String AUTO_RESOLUTION = "auto";

  public static final String ANY_TAG_VALUE = "*";
  public static final String TAG_DELIM = ".";
  public static final String DOT_ESCAPE_CHAR = "~";

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

      .put(Constants.Metrics.Tag.SERVICE, "service")
      .put(Constants.Metrics.Tag.SERVICE_RUNNABLE, "runnable")

      .put(Constants.Metrics.Tag.FLOW, "flow")
      .put(Constants.Metrics.Tag.FLOWLET, "flowlet")
      .put(Constants.Metrics.Tag.FLOWLET_QUEUE, "queue")

      .put(Constants.Metrics.Tag.MAPREDUCE, "mapreduce")
      .put(Constants.Metrics.Tag.MR_TASK_TYPE, "tasktype")

      .put(Constants.Metrics.Tag.WORKFLOW, "workflow")

      .put(Constants.Metrics.Tag.SPARK, "spark")

      .put(Constants.Metrics.Tag.PROCEDURE, "procedure").build();

    tagNameToHuman = mapping;
    humanToTagName = mapping.inverse();
  }

  @Inject
  public MetricsHandler(Authenticator authenticator,
                        final MetricStore metricStore) {
    super(authenticator);

    this.metricStore = metricStore;
  }

  // Deprecate search with context param
  @POST
  @Path("/search")
  public void search(HttpRequest request, HttpResponder responder,
                     @QueryParam("target") String target,
                     @QueryParam("context") String context,
                     @QueryParam("tag") List<String> tags) throws IOException {
    if (target == null) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Required target param is missing");
      return;
    }

    if (target.equals("tag")) {
      tagBasedSearchAndRespond(responder, tags);
    } else if (target.equals("childContext")) {
      // todo supporting 2.8 format - should be removed after deprecation (CDAP-1998)
      searchChildContextAndRespond(responder, context);
    } else if (target.equals("metric")) {
      if (tags.size() > 0) {
        searchMetricAndRespond(responder, tags);
      } else {
        // todo supporting 2.8 format - should be removed after deprecation (CDAP-1998)
        // if there are no tags, we use the context call, however
        // since the returned format is same between for metrics, we wouldn't notice a difference in result.
        searchMetricAndRespond(responder, context);
      }
    } else {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Unknown target param value: " + target);
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

  private List<TagValue> parseTagValues(List<String> tags) {
    List<TagValue> result = Lists.newArrayList();
    for (String tag : tags) {
      // split by ':' and add the tagValue to result list
      String[] tagSplit = tag.split(":", 2);
      if (tagSplit.length == 2) {
        String value = tagSplit[1].equals(ANY_TAG_VALUE) ? null : tagSplit[1];
        result.add(new TagValue(tagSplit[0], value));
      }
    }
    return result;
  }

  // todo supporting 2.8 format - context param should be removed after deprecation (CDAP-1998)
  @POST
  @Path("/query")
  public void query(HttpRequest request, HttpResponder responder,
                    @QueryParam("context") String context,
                    @QueryParam("metric") List<String> metrics,
                    @QueryParam("groupBy") List<String> groupBy,
                    @QueryParam("tag") List<String> tags) throws Exception {

    if (new QueryStringDecoder(request.getUri()).getParameters().isEmpty()) {
      executeBatchQueries(request, responder);
      return;
    }

    if (tags.size() > 0 || (groupBy.size() > 1) || metrics.size() > 1) {
      tagsQuerying(request, responder, tags, metrics, groupBy);
    } else {
      // context querying support for 2.8 compatibility.
      contextQuerying(request, responder, context, metrics.get(0), groupBy.size() > 0 ? groupBy.get(0) : null);
    }
  }

  private void executeBatchQueries(HttpRequest request, HttpResponder responder) {
    if (HttpHeaders.getContentLength(request) > 0) {
      String json = request.getContent().toString(Charsets.UTF_8);
      Map<String, List<QueryRequest>> queries =
        GSON.fromJson(json, new TypeToken<Map<String, List<QueryRequest>>>() { }.getType());
      LOG.trace("Received Queries {}", queries);

      Map<String, List<MetricQueryResult>> queryFinalResponse = Maps.newHashMap();
      for (Map.Entry<String, List<QueryRequest>> query : queries.entrySet()) {
        List<MetricQueryResult> queryResults = Lists.newArrayList();
        for (QueryRequest queryRequest : query.getValue()) {
          queryResults.add(executeQuery(queryRequest));
        }
        queryFinalResponse.put(query.getKey(), queryResults);
      }
      responder.sendJson(HttpResponseStatus.OK, queryFinalResponse);
    } else {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Batch request with empty content");
    }
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

  private void contextQuerying(HttpRequest request, HttpResponder responder,
                               @QueryParam("context") String context,
                               @QueryParam("metric") String metric,
                               @QueryParam("groupBy") String groupBy) {
    try {
      List<String> groupByTags = parseGroupBy(groupBy);
      MetricQueryResult queryResult = executeQuery(request, parseTagValuesAsMap(context),
                                                   groupByTags, ImmutableList.of(metric));
      responder.sendJson(HttpResponseStatus.OK, queryResult);
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    }
  }

  private MetricQueryResult executeQuery(HttpRequest request, Map<String, String> sliceByTags,
                                         List<String> groupByTags, List<String> metrics) {
    try {
      Map<String, String> timeRange =
        getTimeRangeFromQueryParam(new QueryStringDecoder(request.getUri()).getParameters());
      return executeQuery(new QueryRequest(sliceByTags, metrics, groupByTags, timeRange));
    } catch (IllegalArgumentException e) {
      throw Throwables.propagate(e);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private Map<String, String> getTimeRangeFromQueryParam(Map<String, List<String>> queryTimeParams) {
    Map<String, String> timeRange = Maps.newHashMap();
    List<String> paramToCheck = ImmutableList.of(START_TIME, END_TIME, COUNT, INTERPOLATE,
                                                 MAX_INTERPOLATE_GAP, RESOLUTION, AGGREGATE);

    for (String queryParam : paramToCheck) {
      if (queryTimeParams.containsKey(queryParam) && queryTimeParams.get(queryParam).size() > 0) {
        timeRange.put(queryParam, queryTimeParams.get(queryParam).get(0));
      }
    }
    return timeRange;
  }


  private MetricQueryResult executeQuery(QueryRequest queryRequest) {
    try {
      // todo: refactor parsing time range params
      // sets time range, query type, etc.
      MetricQueryParser.MetricDataQueryBuilder builder = new MetricQueryParser.MetricDataQueryBuilder();
      builder.setSliceByTagValues(Maps.<String, String>newHashMap());
      parseTimeRange(queryRequest.getTimeRange(), builder);
      MetricDataQuery queryTimeParams = builder.build();

      Map<String, String> tagsSliceBy = humanToTagNames(transformTagMap(queryRequest.getTags()));

      long startTs = queryTimeParams.getStartTs();
      long endTs = queryTimeParams.getEndTs();

      Collection<MetricTimeSeries> queryResult = Lists.newArrayList();
      for (String metric : queryRequest.getMetrics()) {
        MetricDataQuery query = new MetricDataQuery(startTs, endTs, queryTimeParams.getResolution(),
                                                    queryTimeParams.getLimit(), metric,
                                                    // todo: figure out MetricType
                                                    MetricType.COUNTER, tagsSliceBy,
                                                    transformGroupByTags(queryRequest.getGroupBy()),
                                                    queryTimeParams.getInterpolator());
        Collection<MetricTimeSeries> timeSerieses = metricStore.query(query);

        queryResult.addAll(timeSerieses);
      }

      MetricQueryResult result = decorate(queryResult, startTs, endTs);
      return result;
    } catch (IllegalArgumentException e) {
      throw Throwables.propagate(e);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
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
  private void parseTimeRange(Map<String, String> timeRange, MetricQueryParser.MetricDataQueryBuilder queryTimeParams) {

    if (timeRange.containsKey(AGGREGATE) && timeRange.get(AGGREGATE).equals("true")) {
        queryTimeParams.setStartTs(0);
        queryTimeParams.setEndTs(0);
        queryTimeParams.setResolution(Integer.MAX_VALUE);
        queryTimeParams.setLimit(1);
        return;
    }

    parseInterpolateParam(timeRange, queryTimeParams);

    if (timeRange.containsKey(START_TIME)) {
      queryTimeParams.setStartTs(TimeMathParser.parseTime(timeRange.get("start")));
    }
    if (timeRange.containsKey(END_TIME)) {
      queryTimeParams.setEndTs(TimeMathParser.parseTime(timeRange.get("end")));
    }
    int resolutionInterval = parseResolutionParam(timeRange, queryTimeParams);
    parseCountParam(timeRange, queryTimeParams, resolutionInterval);
  }

  /**
   * parse and set interpolator based on timeRange parameters
   */
  private void parseInterpolateParam(Map<String, String> timeRange,
                                     MetricQueryParser.MetricDataQueryBuilder queryTimeParams) {

    if (timeRange.containsKey(INTERPOLATE)) {
      String interpolatorType = timeRange.get(INTERPOLATE);
      // timeLimit used in case there is a big gap in the data and we don't want to interpolate points.
      // the limit defines how big the gap has to be in seconds before we just say they're all zeroes.
      long timeLimit = timeRange.containsKey(MAX_INTERPOLATE_GAP)
        ? Long.parseLong(timeRange.get(MAX_INTERPOLATE_GAP))
        : Long.MAX_VALUE;

      if (STEP_INTERPOLATOR.equals(interpolatorType)) {
        queryTimeParams.setInterpolator(new Interpolators.Step(timeLimit));
      } else if (LINEAR_INTERPOLATOR.equals(interpolatorType)) {
        queryTimeParams.setInterpolator(new Interpolators.Linear(timeLimit));
      }
    }
  }

  /**
   * determines and sets resolution based on timeRange parameters
   */
  private int parseResolutionParam(Map<String, String> timeRange,
                                   MetricQueryParser.MetricDataQueryBuilder queryTimeParams) {
    int resolutionInterval = 1;
    // if resolution is present, if auto -> check if start and end time are available, if not throw an exception
    // if available based on time difference we select a resolution
    if (timeRange.containsKey(RESOLUTION)) {
      if (timeRange.get(RESOLUTION).equals(AUTO_RESOLUTION)) {
        if (!timeRange.containsKey(START_TIME) || !timeRange.containsKey(END_TIME)) {
          throw new IllegalArgumentException("Need start and end timestamp for auto resolution determination");
        }
        long difference = Long.parseLong(timeRange.get(END_TIME)) - Long.parseLong(timeRange.get(START_TIME));
        if (difference > MetricsConstants.MAX_HOUR_RESOLUTION_QUERY_INTERVAL) {
          queryTimeParams.setResolution(3600);
        } else if (difference > MetricsConstants.MAX_MINUTE_RESOLUTION_QUERY_INTERVAL) {
          queryTimeParams.setResolution(60);
        } else {
          queryTimeParams.setResolution(1);
        }
      } else {
        // if not auto, check if the given resolution matches available resolutions that we support.
        resolutionInterval = TimeMathParser.resolutionInSeconds(timeRange.get(RESOLUTION));
        if (!((resolutionInterval == Integer.MAX_VALUE) || (resolutionInterval == 3600) ||
          (resolutionInterval == 60) || (resolutionInterval == 1))) {
          throw new IllegalArgumentException("Resolution interval not supported, only 1 second, 1 minute and " +
                                               "1 hour resolutions are supported currently");
        }
        queryTimeParams.setResolution(resolutionInterval);
      }
    } else {
      // if no resolution is available, use 1 second as default
      queryTimeParams.setResolution(resolutionInterval);
    }
    return resolutionInterval;
  }

  /**
   * parses and sets count of returned results or sets start or end time based on count
   */
  private void parseCountParam(Map<String, String> timeRange,
                               MetricQueryParser.MetricDataQueryBuilder queryTimeParams, int resolutionInterval) {

    // if count is available, we set count , and if only one of the startTime or endTime parameter is available,
    // we determine the other based on count and resolution
    if (timeRange.containsKey(COUNT)) {
      queryTimeParams.setLimit((int) TimeMathParser.parseTime(timeRange.get(COUNT)));
      if (timeRange.containsKey(START_TIME) && !timeRange.containsKey(END_TIME)) {
        long endTs = Long.parseLong(timeRange.get(START_TIME)) +
          Integer.parseInt(timeRange.get(COUNT)) * resolutionInterval;
        queryTimeParams.setEndTs(endTs);
      } else  if (timeRange.containsKey(END_TIME) && !timeRange.containsKey(START_TIME)) {
        long endTs = Long.parseLong(timeRange.get(END_TIME)) -
          Integer.parseInt(timeRange.get(COUNT)) * resolutionInterval;
        queryTimeParams.setEndTs(endTs);
      }
    } else {
      // if count is not available, we need start and end times to determine count otherwise we throw exception
      if (timeRange.containsKey(START_TIME) && timeRange.containsKey(END_TIME)) {
        long endTime = TimeMathParser.parseTime(timeRange.get(END_TIME));
        long startTime = TimeMathParser.parseTime(timeRange.get(START_TIME));

        int count = (int) (((endTime / resolutionInterval * resolutionInterval) -
          (startTime / resolutionInterval * resolutionInterval)) / resolutionInterval + 1);
        queryTimeParams.setLimit(count);
      } else {
        throw new IllegalArgumentException("At least two of count/start/end parameters " +
                                             "are required for time-range queries ");
      }
    }
  }

  private List<String> parseGroupBy(String groupBy) {
    // groupBy tags are comma separated
    return  (groupBy == null) ? Lists.<String>newArrayList() :
      humanToTagNamesGroupBy(Lists.newArrayList(Splitter.on(",").split(groupBy).iterator()));
  }

  private List<String> humanToTagNamesGroupBy(List<String> groupByTags) {
    return Lists.transform(groupByTags, new Function<String, String>() {
      @Nullable
      @Override
      public String apply(@Nullable String input) {
        return humanToTagName(input);
      }
    });
  }

  private Map<String, String> parseTagValuesAsMap(@Nullable String context) {
    if (context == null) {
      return new HashMap<String, String>();
    }
    String[] tagValues = context.split("\\.");

    // order matters
    Map<String, String> result = Maps.newLinkedHashMap();
    for (int i = 0; i < tagValues.length; i += 2) {
      String name = tagValues[i];
      // if odd number, the value for last tag is assumed to be null
      String val = i + 1 < tagValues.length ? tagValues[i + 1] : null;
      if (ANY_TAG_VALUE.equals(val)) {
        val = null;
      }
      if (val != null) {
        val = decodeTag(val);
      }
      result.put(decodeTag(name), val);
    }

    return result;
  }

  private Map<String, String> parseTagValuesAsMap(List<String> tags) {
    List<TagValue> tagValues = parseTagValues(tags);

    Map<String, String> result = Maps.newHashMap();
    for (TagValue tagValue : tagValues) {
      result.put(tagValue.getTagName(), tagValue.getValue());
    }
    return result;
  }

  private String decodeTag(String val) {
    // we use "." as separator between tags, so if tag name or value has dot in it, we encode it with "~"
    return val.replaceAll(DOT_ESCAPE_CHAR, ".");
  }

  private String encodeTag(String val) {
    // we use "." as separator between tags, so if tag name or value has dot in it, we encode it with "~"
    return val.replaceAll("\\.", DOT_ESCAPE_CHAR);
  }

  private void searchMetricAndRespond(HttpResponder responder, String context) {
    try {
      responder.sendJson(HttpResponseStatus.OK, searchMetric(context));
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.warn("Exception while retrieving available metrics", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void tagBasedSearchAndRespond(HttpResponder responder, List<String> tags) {
    try {
      // we want to search the entire range, so startTimestamp is '0' and end Timestamp is Integer.MAX_VALUE and
      // limit is -1 , to include the entire search result.
      MetricSearchQuery searchQuery = new MetricSearchQuery(0, Integer.MAX_VALUE, -1,
                                                            humanToTagNames(parseTagValues(tags)));
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

  private void searchChildContextAndRespond(HttpResponder responder, String context) {
    try {
      responder.sendJson(HttpResponseStatus.OK, searchChildContext(context));
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.warn("Exception while retrieving contexts", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private List<TagValue> parseTagValues(String contextPrefix) throws Exception {
    Map<String, String> map = parseTagValuesAsMap(contextPrefix);
    List<TagValue> contextTags = Lists.newArrayList();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      contextTags.add(new TagValue(entry.getKey(), entry.getValue()));
    }

    return contextTags;
  }

  private Collection<String> searchChildContext(String contextPrefix) throws Exception {
    List<TagValue> tagValues = parseTagValues(contextPrefix);

    // we want to search the entire range, so startTimestamp is '0' and endTimestamp is Integer.MAX_VALUE and
    // limit is -1 , to include the entire search result.
    MetricSearchQuery searchQuery = new MetricSearchQuery(0, Integer.MAX_VALUE, -1,
                                                          humanToTagNames(tagValues));
    Collection<TagValue> nextTags = metricStore.findNextAvailableTags(searchQuery);

    contextPrefix = toCanonicalContext(tagValues);
    Collection<String> result = Lists.newArrayList();
    for (TagValue tag : nextTags) {
      // for now, if tag value is null, we use ANY_TAG_VALUE as returned for convenience: this allows to easy build UI
      // and do simple copy-pasting when accessing HTTP endpoint via e.g. curl
      String value = tag.getValue() == null ? ANY_TAG_VALUE : tag.getValue();
      String name = tagNameToHuman(tag);
      String tagValue = encodeTag(name)  + TAG_DELIM + encodeTag(value);
      String resultTag = contextPrefix.length() == 0 ? tagValue : contextPrefix + TAG_DELIM + tagValue;
      result.add(resultTag);
    }
    return result;
  }

  private List<TagValue> tagValuesToHuman(Collection<TagValue> tagValues) {
    List<TagValue> result = Lists.newArrayList();
    for (TagValue tagValue : tagValues) {
      String human = tagNameToHuman.get(tagValue.getTagName());
      human = human != null ? human : tagValue.getTagName();
      String value = tagValue.getValue() == null ? ANY_TAG_VALUE : tagValue.getValue();
      result.add(new TagValue(human, value));
    }
    return result;
  }

  private String tagNameToHuman(TagValue tag) {
    String human = tagNameToHuman.get(tag.getTagName());
    return human != null ? human : tag.getTagName();
  }

  private List<TagValue> humanToTagNames(List<TagValue> tagValues) {
    List<TagValue> result = Lists.newArrayList();
    for (TagValue tagValue : tagValues) {
      String tagName = humanToTagName(tagValue.getTagName());
      result.add(new TagValue(tagName, tagValue.getValue()));
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

  private String toCanonicalContext(List<TagValue> tagValues) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (TagValue tv : tagValues) {
      if (!first) {
        sb.append(TAG_DELIM);
      }
      first = false;
      sb.append(encodeTag(tv.getTagName())).append(TAG_DELIM)
        .append(tv.getValue() == null ? ANY_TAG_VALUE : encodeTag(tv.getValue()));
    }
    return sb.toString();
  }

  private Collection<String> searchMetric(String contextPrefix) throws Exception {
    List<TagValue> tagValues = humanToTagNames(parseTagValues(contextPrefix));
    return getMetrics(tagValues);
  }

  private Collection<String> getMetrics(List<TagValue> tagValues) throws Exception {
    // we want to search the entire range, so startTimestamp is '0' and end Timestamp is Integer.MAX_VALUE and
    // limit is -1 , to include the entire search result.
    MetricSearchQuery searchQuery =
      new MetricSearchQuery(0, Integer.MAX_VALUE, -1, tagValues);
    Collection<String> metricNames = metricStore.findMetricNames(searchQuery);
    return Lists.newArrayList(Iterables.filter(metricNames, Predicates.notNull()));
  }

  private MetricQueryResult decorate(Collection<MetricTimeSeries> series, long startTs, long endTs) {
    MetricQueryResult.TimeSeries[] serieses = new MetricQueryResult.TimeSeries[series.size()];
    int i = 0;
    for (MetricTimeSeries timeSeries : series) {
      MetricQueryResult.TimeValue[] timeValues = decorate(timeSeries.getTimeValues());
      serieses[i++] = new MetricQueryResult.TimeSeries(timeSeries.getMetricName(),
                                                       tagNamesToHuman(timeSeries.getTagValues()), timeValues);
    }
    return new MetricQueryResult(startTs, endTs, serieses);
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
   * Format for metrics query in batched queries
   */
  /*class QueryRequest {
    Map<String, String> tags;
    List<String> metrics;
    List<String> groupBy;
    Map<String, String> timeRange;

    QueryRequest(Map<String, String> tags, List<String> metrics, List<String> groupBy,
                 Map<String, String> timeRange) {
      this.tags = tags;
      this.metrics = metrics;
      this.groupBy = groupBy;
      this.timeRange = timeRange;
    }

    private Map<String, String> parseTags(Map<String, String> tags) {
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

    public Map<String, String> getTags() {
      return parseTags(tags);
    }

    public List<String> getMetrics() {
      return metrics;
    }

    public List<String> getGroupBy() {
      return Lists.transform(groupBy, new Function<String, String>() {
        @Nullable
        @Override
        public String apply(@Nullable String input) {
            String replacement = humanToTagName.get(input);
            return replacement != null ? replacement : input;
        }
      });
    }

    public Map<String, String> getTimeRange() {
      return timeRange;
    }
  }*/
}
