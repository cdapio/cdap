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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricTags;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.metrics.store.MetricStore;
import co.cask.cdap.metrics.store.cube.CubeExploreQuery;
import co.cask.cdap.metrics.store.cube.CubeQuery;
import co.cask.cdap.metrics.store.cube.TimeSeries;
import co.cask.cdap.metrics.store.timeseries.MeasureType;
import co.cask.cdap.metrics.store.timeseries.TagValue;
import co.cask.cdap.metrics.store.timeseries.TimeValue;
import co.cask.http.HttpResponder;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
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
import java.util.Iterator;
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

  private final MetricStore metricStore;

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
      MetricQueryParser.CubeQueryBuilder builder = new MetricQueryParser.CubeQueryBuilder();
      MetricQueryParser.parseQueryString(new URI(request.getUri()), builder);
      builder.setSliceByTagValues(Maps.<String, String>newHashMap());
      CubeQuery queryTimeParams = builder.build();

      Map<String, String> tagsSliceBy = parseTagValuesAsMap(context);
      List<String> groupByTags = parseGroupBy(groupBy);

      long startTs = queryTimeParams.getStartTs();
      long endTs = queryTimeParams.getEndTs();

      CubeQuery query = new CubeQuery(startTs, endTs,
                                      queryTimeParams.getResolution(), metric,
                                          // todo: figure out MeasureType
                                      MeasureType.COUNTER, tagsSliceBy, groupByTags);

      Collection<TimeSeries> queryResult = metricStore.query(query);
      MetricQueryResult result = decorate(queryResult, startTs, endTs);

      responder.sendJson(HttpResponseStatus.OK, result);
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    }
  }

  private List<String> parseGroupBy(String groupBy) {
    // groupBy tags are comma separated
    return (groupBy == null) ? Lists.<String>newArrayList() :
    Lists.newArrayList(
     Iterables.transform(Splitter.on(",").split(groupBy),  new Function<String, String>() {
       @Override
       public String apply(String tag) {
         MetricTags tagKey = MetricTags.valueOf(tag);
         Preconditions.checkNotNull(tagKey);
         return tagKey.getCodeName();
       }
     }));
  }

  private Map<String, String> parseTagValuesAsMap(@Nullable String context) {
    if (context == null) {
      return new HashMap<String, String>();
    }
    String[] tagValues = context.split("\\.");

    // order matters
    Map<String, String> result = Maps.newLinkedHashMap();
    for (int i = 0; i < tagValues.length; i += 2) {
      String tag = tagValues[i];
      MetricTags tagKey = MetricTags.valueOf(tag.toUpperCase());
      Preconditions.checkNotNull(tagKey);
      tag = tagKey.getCodeName();
      // if odd number, the value for last tag is assumed to be null
      String val = i + 1 < tagValues.length ? tagValues[i + 1] : null;
      if (ANY_TAG_VALUE.equals(val)) {
        val = null;
      }
      result.put(tag, val);
    }

    return result;
  }

  private void searchMetricAndRespond(HttpResponder responder, String context) {
    try {
      responder.sendJson(HttpResponseStatus.OK, searchMetric(context));
    } catch (Exception e) {
      LOG.warn("Exception while retrieving available metrics", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void searchChildContextAndRespond(HttpResponder responder, String context) {
    try {
      responder.sendJson(HttpResponseStatus.OK, searchChildContext(context));
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
//    toCanonicalContext(tagValues);
//    contextPrefix = toCanonicalContext(tagValues);
    CubeExploreQuery searchQuery = new CubeExploreQuery(0, Integer.MAX_VALUE - 1, 1, -1, tagValues);
    Collection<TagValue> nextTags = metricStore.findNextAvailableTags(searchQuery);
    Collection<String> result = Lists.newArrayList();
    for (TagValue tag : nextTags) {
      if (tag.getValue() == null) {
        continue;
      }
      MetricTags tagKey = MetricTags.valueOfCodeName(tag.getTagName());
      String tagValue = tagKey == null ? tag.getTagName() : tagKey.name().toLowerCase()  + TAG_DELIM + tag.getValue();
      String resultTag = (contextPrefix == null || contextPrefix.length() == 0) ?
        tagValue : contextPrefix + TAG_DELIM + tagValue;
      result.add(resultTag);
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
      sb.append(tv.getTagName()).append(TAG_DELIM).append(tv.getValue() == null ? ANY_TAG_VALUE : tv.getValue());
    }
    return sb.toString();
  }

  private Collection<String> searchMetric(String contextPrefix) throws Exception {
    CubeExploreQuery searchQuery = new CubeExploreQuery(0, Integer.MAX_VALUE - 1, 1, -1, parseTagValues(contextPrefix));
    Collection<String> metricNames = metricStore.findMetricNames(searchQuery);
    return Lists.newArrayList(Iterables.filter(metricNames, Predicates.notNull()));
  }

  private MetricQueryResult decorate(Collection<TimeSeries> timeSerieses, long startTs, long endTs) {
    MetricQueryResult.TimeSeries[] serieses = new MetricQueryResult.TimeSeries[timeSerieses.size()];
    int i = 0;
    for (TimeSeries timeSeries : timeSerieses) {
      MetricQueryResult.TimeValue[] timeValues = decorate(timeSeries.getTimeValues());
      serieses[i++] = new MetricQueryResult.TimeSeries(timeSeries.getMeasureName(),
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
