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

import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.api.metrics.TimeValue;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
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
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(TagValue.class, new TagValueSerializer())
    .create();

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

    if (tags.size() > 0) {
      if ("tag".equals(target)) {
        searchTargetContextAndRespond(responder, tags);
      } else if ("metric".equals(target)) {
        searchMetricAndRespond(responder, tags);
      } else {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Unknown target param value: " + target);
      }
    } else {
     // 2.8 API call
      if ("childContext".equals(target)) {
        searchChildContextAndRespond(responder, context);
      } else if ("metric".equals(target)) {
        searchMetricAndRespond(responder, context);
      } else {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Unknown target param value: " + target);
      }
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
        List<MetricQueryResult> queryResultSet = Lists.newArrayList();
        for (QueryRequest queryRequest : query.getValue()) {
          queryResultSet.add(executeQuery(queryRequest));
        }
        queryFinalResponse.put(query.getKey(), queryResultSet);
      }
      responder.sendJson(HttpResponseStatus.OK, queryFinalResponse);
    } else {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Batch request with empty content");
    }
  }

  private void tagsQuerying(HttpRequest request, HttpResponder responder, List<String> tags, List<String> metrics,
                            List<String> groupByTags) {
    try {
      List<MetricQueryResult> results = Lists.newArrayList();
      for (String metric : metrics) {
        results.add(executeQuery(request, parseTagValuesAsMap(tags), groupByTags, metric));
      }
      responder.sendJson(HttpResponseStatus.OK, results);
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
                                                   groupByTags, metric);
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
                                         List<String> groupByTags, String metric) {
    try {
      // todo: refactor parsing time range params
      // sets time range, query type, etc.
      MetricQueryParser.MetricDataQueryBuilder builder = new MetricQueryParser.MetricDataQueryBuilder();
      MetricQueryParser.parseQueryString(new URI(request.getUri()), builder);
      builder.setSliceByTagValues(Maps.<String, String>newHashMap());
      MetricDataQuery queryTimeParams = builder.build();

      Map<String, String> tagsSliceBy = humanToTagNames(sliceByTags);

      long startTs = queryTimeParams.getStartTs();
      long endTs = queryTimeParams.getEndTs();

      MetricDataQuery query = new MetricDataQuery(startTs, endTs, queryTimeParams.getResolution(), metric,
                                                  // todo: figure out MetricType
                                                  MetricType.COUNTER, tagsSliceBy, groupByTags);

      Collection<MetricTimeSeries> queryResult = metricStore.query(query);
      MetricQueryResult result = decorate(queryResult, startTs, endTs);
      return result;
    } catch (IllegalArgumentException e) {
      throw Throwables.propagate(e);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }


  private MetricQueryResult executeQuery(QueryRequest queryRequest) {
    try {
      // todo: refactor parsing time range params
      // sets time range, query type, etc.
      MetricQueryParser.MetricDataQueryBuilder builder = new MetricQueryParser.MetricDataQueryBuilder();
      builder.setSliceByTagValues(Maps.<String, String>newHashMap());
      parseTimeRange(queryRequest.getTimeRange(), queryRequest.getResolution(), builder);
      MetricDataQuery queryTimeParams = builder.build();

      Map<String, String> tagsSliceBy = humanToTagNames(queryRequest.getTags());

      long startTs = queryTimeParams.getStartTs();
      long endTs = queryTimeParams.getEndTs();

      Collection<MetricTimeSeries> queryResult = Lists.newArrayList();
      for (String metric : queryRequest.getMetrics()) {
        MetricDataQuery query = new MetricDataQuery(startTs, endTs, queryTimeParams.getResolution(), metric,
                                                    // todo: figure out MetricType
                                                    MetricType.COUNTER, tagsSliceBy, queryRequest.getGroupBy());
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

  private void parseTimeRange(Map<String, String> timeRange, String resolution,
                              MetricQueryParser.MetricDataQueryBuilder queryTimeParams) {
    long startTs = 0, endTs = 0;
    int count = 0;
    int resolutionInterval = 1;
    if (resolution != null) {
      resolutionInterval = (int) TimeMathParser.parseTime(resolution);
    }
    for (Map.Entry<String, String> entry : timeRange.entrySet()) {
      if (entry.getKey().equals("aggregate") && entry.getValue().equals("true")) {
        queryTimeParams.setStartTs(0);
        queryTimeParams.setEndTs(0);
        queryTimeParams.setResolution(Integer.MAX_VALUE);
        queryTimeParams.setLimit(1);
        return;
      }
      if (entry.getKey().equals("start")) {
        startTs = TimeMathParser.parseTime(entry.getValue());
        queryTimeParams.setStartTs(startTs);
      }
      if (entry.getKey().equals("end")) {
        endTs = TimeMathParser.parseTime(entry.getValue());
        queryTimeParams.setEndTs(endTs);
      }
      if (entry.getKey().equals("count")) {
        count = Integer.parseInt(entry.getValue());
        queryTimeParams.setLimit(count);
      }
    }
    queryTimeParams.setResolution(resolutionInterval);
    if (endTs == 0 && startTs > 0 && count > 0) {
      queryTimeParams.setEndTs(startTs + (count * resolutionInterval));
    } else if (startTs == 0 && endTs > 0 & count > 0) {
      queryTimeParams.setStartTs(endTs - (count * resolutionInterval));
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
    Map<String, String> result = Maps.newHashMap();
    for (String tag : tags) {
      String[] tagSplit = tag.split(":", 2);
      result.put(tagSplit[0], ANY_TAG_VALUE.equals(tagSplit[1]) ? null : tagSplit[1]);
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

  private void searchTargetContextAndRespond(HttpResponder responder, List<String> tags) {
    try {
      MetricSearchQuery searchQuery = new MetricSearchQuery(0, Integer.MAX_VALUE - 1, -1,
                                                            humanToTagNames(parseTagValues(tags)));
      Collection<TagValue> nextTags = metricStore.findNextAvailableTags(searchQuery);
      responder.sendJson(HttpResponseStatus.OK, nextTags, new TypeToken<Collection<TagValue>>() { }.getType(), GSON);
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

    MetricSearchQuery searchQuery = new MetricSearchQuery(0, Integer.MAX_VALUE - 1, -1,
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
    MetricSearchQuery searchQuery =
      new MetricSearchQuery(0, Integer.MAX_VALUE - 1, -1, tagValues);
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
   * Parsing metrics request queries
   */
  class QueryRequest {
    Map<String, String> tags;
    List<String> metrics;
    List<String> groupBy;
    Map<String, String> timeRange;
    String resolution;

    QueryRequest(Map<String, String> tags, List<String> metrics, List<String> groupBy,
                 Map<String, String> timeRange, String resolution) {
      this.tags = tags;
      this.metrics = metrics;
      this.groupBy = groupBy;
      this.timeRange = timeRange;
      this.resolution = resolution;
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

    public String getResolution() {
      return resolution;
    }

    public Map<String, String> getTimeRange() {
      return timeRange;
    }
  }

  /**
   * Adapter for {@link co.cask.cdap.api.metrics.TagValue}
   */
  private static final class TagValueSerializer implements JsonSerializer<TagValue> {
    @Override
    public JsonElement serialize(TagValue tagValue, Type typeOfSrc, JsonSerializationContext context) {
      String human = tagNameToHuman.get(tagValue.getTagName());
      human = human != null ? human : tagValue.getTagName();
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("name", human);
      jsonObject.addProperty("value", tagValue.getValue());
      return jsonObject;
    }
  }
}
