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
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.http.HttpResponder;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
                     @QueryParam("context") String context) throws IOException {
    if (target == null) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Required target param is missing");
      return;
    }

    if ("childContext".equals(target)) {
      searchChildContextAndRespond(responder, context);
    } else if ("metric".equals(target)) {
      searchMetricAndRespond(responder, context);
    } else {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Unknown target param value: " + target);
    }
  }

  @POST
  @Path("/query")
  public void query(HttpRequest request, HttpResponder responder,
                    @QueryParam("context") String context,
                    @QueryParam("metric") String metric,
                    @QueryParam("groupBy") String groupBy) throws Exception {
    try {
      // todo: refactor parsing time range params
      // sets time range, query type, etc.
      MetricQueryParser.MetricDataQueryBuilder builder = new MetricQueryParser.MetricDataQueryBuilder();
      MetricQueryParser.parseQueryString(new URI(request.getUri()), builder);
      builder.setSliceByTagValues(Maps.<String, String>newHashMap());
      MetricDataQuery queryTimeParams = builder.build();

      Map<String, String> tagsSliceBy = humanToTagNames(parseTagValuesAsMap(context));

      List<String> groupByTags = parseGroupBy(groupBy);

      long startTs = queryTimeParams.getStartTs();
      long endTs = queryTimeParams.getEndTs();

      MetricDataQuery query = new MetricDataQuery(startTs, endTs, queryTimeParams.getResolution(), metric,
                                                  // todo: figure out MetricType
                                                  MetricType.COUNTER, tagsSliceBy, groupByTags);

      Collection<MetricTimeSeries> queryResult = metricStore.query(query);
      MetricQueryResult result = decorate(queryResult, startTs, endTs);

      responder.sendJson(HttpResponseStatus.OK, result);
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    }
  }

  private List<String> parseGroupBy(String groupBy) {
    // groupBy tags are comma separated
    return (groupBy == null) ? Lists.<String>newArrayList() :
      Lists.newArrayList(Splitter.on(",").split(groupBy).iterator());
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
                                                       timeSeries.getTagValues(), timeValues);
    }
    return new MetricQueryResult(startTs, endTs, serieses);
  }

  private MetricQueryResult.TimeValue[] decorate(List<TimeValue> points) {
    MetricQueryResult.TimeValue[] timeValues = new MetricQueryResult.TimeValue[points.size()];
    int k = 0;
    for (TimeValue timeValue : points) {
      timeValues[k++] = new MetricQueryResult.TimeValue(timeValue.getTimestamp(), timeValue.getValue());
    }
    return timeValues;
  }
}
