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
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Search metrics handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/metrics")
public class MetricsHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsDiscoveryHandler.class);

  private final MetricStore metricStore;
  private final List<String> tagMappings;

  @Inject
  public MetricsHandler(Authenticator authenticator,
                        final MetricStore metricStore) {
    super(authenticator);

    this.metricStore = metricStore;
    tagMappings = ImmutableList.of("ns", "app", "ptp", "prg", "pr2", "pr3", "pr4", "ds");
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
                     @QueryParam("metric") String metric) throws Exception {
    try {
      // todo: refactor parsing time range params
      // sets time range, query type, etc.
      MetricQueryParser.CubeQueryBuilder builder = new MetricQueryParser.CubeQueryBuilder();
      MetricQueryParser.parseQueryString(new URI(request.getUri()), builder);
      builder.setSliceByTagValues(Maps.<String, String>newHashMap());
      CubeQuery queryTimeParams = builder.build();

      // todo: what if context is null?
      String[] tagValues = context.split("\\.");
      // todo: validate even number of parts?

      Map<String, String> tagsSliceBy = Maps.newHashMap();
      for (int i = 0; i < tagValues.length - 1; i += 2) {
        tagsSliceBy.put(tagValues[i], tagValues[i + 1]);
      }

      long startTs = queryTimeParams.getStartTs();
      long endTs = queryTimeParams.getEndTs();
      CubeQuery query = new CubeQuery(startTs, endTs,
                                      queryTimeParams.getResolution(), metric,
                                          // todo: figure out MeasureType
                                      MeasureType.COUNTER, tagsSliceBy, new ArrayList<String>());

      Collection<TimeSeries> queryResult = metricStore.query(query);
      MetricQueryResult result = decorate(queryResult, startTs, endTs);

      responder.sendJson(HttpResponseStatus.OK, result);
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    }
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

  private List<TagValue> getContext(String contextPrefix) throws Exception {
    List<String> contextParts = Lists.newArrayList();
    if (contextPrefix != null) {
      contextParts = Lists.newArrayList(Splitter.on('.').split(contextPrefix));
    }
    List<TagValue> contextTags = Lists.newArrayList();
    for (int i = 0; i < contextParts.size(); i++) {
      contextTags.add(new TagValue(tagMappings.get(i), contextParts.get(i)));
    }

    if (contextTags.size() > 3) {
      //todo : adding null for runId,should we support searching with runId ?
      contextTags.add(4, new TagValue("run", null));
    }
    return contextTags;
  }

  private Collection<String> searchChildContext(String contextPrefix) throws Exception {
    CubeExploreQuery searchQuery = new CubeExploreQuery(0, Integer.MAX_VALUE - 1, 1, -1, getContext(contextPrefix));
    Collection<TagValue> nextTags = metricStore.findNextAvailableTags(searchQuery);
    Collection<String> result = Lists.newArrayList();
    for (TagValue tag : nextTags) {
      if (tag.getValue() == null) {
        continue;
      }
      String resultTag = contextPrefix == null ? tag.getValue() : contextPrefix + "." + tag.getValue();
      result.add(resultTag);
    }
    return result;
  }

  private Collection<String> searchMetric(String contextPrefix) throws Exception {
    CubeExploreQuery searchQuery = new CubeExploreQuery(0, Integer.MAX_VALUE - 1, 1, -1, getContext(contextPrefix));
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
