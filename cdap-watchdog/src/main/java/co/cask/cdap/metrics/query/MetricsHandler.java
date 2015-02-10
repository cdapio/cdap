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
import co.cask.cdap.metrics.store.cube.CubeQuery;
import co.cask.cdap.metrics.store.cube.TimeSeries;
import co.cask.cdap.metrics.store.timeseries.MeasureType;
import co.cask.http.HttpResponder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
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
                     @QueryParam("metric") String metric) throws Exception {
    try {
      // todo: refactor parsing time range params
      // sets time range, query type, etc.
      MetricQueryParser.CubeQueryBuilder builder = new MetricQueryParser.CubeQueryBuilder();
      MetricQueryParser.parseQueryString(new URI(request.getUri()), builder);
      CubeQuery queryTimeParams = builder.build();

      // todo: what if context is null?
      String[] tagValues = context.split("\\.");
      // todo: validate even number of parts?
  
      Map<String, String> tagsSliceBy = Maps.newHashMap();
      for (int i = 0; i < tagValues.length - 1; i += 2) {
        tagsSliceBy.put(tagValues[i], tagValues[i + 1]);
      }
  
      CubeQuery query = new CubeQuery(queryTimeParams.getStartTs(), queryTimeParams.getEndTs(),
                                      queryTimeParams.getResolution(), metric,
                                          // todo: figure out MeasureType
                                      MeasureType.COUNTER, tagsSliceBy, new ArrayList<String>());
  
      Collection<TimeSeries> result = metricStore.query(query);
      responder.sendJson(HttpResponseStatus.OK, result);
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
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

  private Collection<String> searchChildContext(String contextPrefix) throws Exception {
    SortedSet<String> nextLevelContexts = Sets.newTreeSet();

    // todo: implement

    return nextLevelContexts;
  }

  private Set<String> searchMetric(String contextPrefix) throws Exception {
    SortedSet<String> metrics = Sets.newTreeSet();

    // todo: implement

    return metrics;
  }
}
