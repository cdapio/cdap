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
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.metrics.data.MetricsScanQuery;
import co.cask.cdap.metrics.data.MetricsScanQueryBuilder;
import co.cask.cdap.metrics.data.MetricsTableFactory;
import co.cask.cdap.metrics.data.TimeSeriesTable;
import co.cask.http.HttpResponder;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Handler to list metrics top-level-contexts , get next-level contexts and obtain metrics at a given context.
 */

//todo : when MetricsQueryHandler class supports namespaces, these endpoints with conflict,
// we might want to combine this logic into MetricsQueryHandler or change the endpoint here based on API Review.
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/metrics")
public class MetricsSearchHandler extends  BaseMetricsHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsDiscoveryHandler.class);
  private final Supplier<Map<MetricsScope, TimeSeriesTable>> timeSeriesTables;

  // NOTE: Hour is the lowest resolution and also has the highest TTL compared to minute and second resolutions.
  // highest TTL ensures, this is good for querying all available metrics and also has fewer rows of data points.
  private static final int LOWEST_RESOLUTION = 3600;

  @Inject
  public MetricsSearchHandler(Authenticator authenticator, final MetricsTableFactory metricsTableFactory) {
    super(authenticator);

    this.timeSeriesTables = Suppliers.memoize(new Supplier<Map<MetricsScope, TimeSeriesTable>>() {
      @Override
      public Map<MetricsScope, TimeSeriesTable> get() {
        Map<MetricsScope, TimeSeriesTable> map = Maps.newHashMap();
        for (MetricsScope scope : MetricsScope.values()) {
          map.put(scope, metricsTableFactory.createTimeSeries(scope.name(), LOWEST_RESOLUTION));
        }
        return map;
      }
    });
  }


  /**
   * Returns all the unique elements available in the first context
   */
  @GET
  @Path("/{scope}")
  public void listFirstContexts(HttpRequest request, HttpResponder responder,
                                @PathParam("scope") final String scope,
                                @QueryParam("search") String search) throws IOException {
    try {
      if (search == null) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "please provide search=childContext " +
          "for getting next context");
        return;
      }
      MetricsScope metricsScope = MetricsScope.valueOf(scope.toUpperCase());
      if (metricsScope == null) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "only" + MetricsScope.SYSTEM + " and " +
          MetricsScope.USER + "are supported currently");
        return;
      }

      responder.sendJson(HttpResponseStatus.OK, getNextContext(metricsScope, null));
    } catch (OperationException e) {
      LOG.warn("Exception while retrieving contexts", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns all the unique elements available in the context after the given context prefix
   */
  @GET
  @Path("/{scope}/{context}")
  public void listContextsByPrefix(HttpRequest request, HttpResponder responder,
                                   @PathParam("scope") final String scope,
                                   @PathParam("context") final String context,
                                   @QueryParam("search") String search) throws IOException {
    try {
      if (search == null) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "please provide search=childContext " +
          "for getting next context");
        return;
      }
      MetricsScope metricsScope = MetricsScope.valueOf(scope.toUpperCase());
      if (metricsScope == null) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "only" + MetricsScope.SYSTEM + " and " +
          MetricsScope.USER + "are supported currently");
        return;
      }
      responder.sendJson(HttpResponseStatus.OK, getNextContext(metricsScope, context));
    } catch (OperationException e) {
      LOG.warn("Exception while retrieving contexts", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns all the unique metrics in the given context
   */
  @GET
  @Path("/{scope}/{context}/metrics")
  public void listContextMetrics(HttpRequest request, HttpResponder responder,
                                 @PathParam("scope") final String scope,
                                 @PathParam("context") final String context) throws IOException {
    try {
      MetricsScope metricsScope = MetricsScope.valueOf(scope.toUpperCase());
      if (metricsScope == null) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "only" + MetricsScope.SYSTEM + " and " +
          MetricsScope.USER + "are supported currently");
        return;
      }
      responder.sendJson(HttpResponseStatus.OK, getAvailableMetricNames(metricsScope, context));
    } catch (OperationException e) {
      LOG.warn("Exception while retrieving available metrics", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private Set<String> getNextContext(MetricsScope scope, String contextPrefix) throws OperationException {
    SortedSet<String> nextLevelContexts = Sets.newTreeSet();
    TimeSeriesTable table = timeSeriesTables.get().get(scope);
    MetricsScanQuery query = new MetricsScanQueryBuilder().setContext(contextPrefix).
      allowEmptyMetric().build(-1, -1);

    List<String> results = table.getNextLevelContexts(query);
    for (String nextContext : results) {
      if (contextPrefix == null) {
        nextLevelContexts.add(nextContext.substring(0, nextContext.indexOf(".")));
      } else {
        String context = nextContext.substring(contextPrefix.length() + 1);
        nextLevelContexts.add(context.substring(0, context.indexOf(".")));
      }
    }

    return nextLevelContexts;
  }

  private Set<String> getAvailableMetricNames(MetricsScope scope, String contextPrefix) throws OperationException {
    SortedSet<String> metrics = Sets.newTreeSet();
    TimeSeriesTable table = timeSeriesTables.get().get(scope);
    MetricsScanQuery query = new MetricsScanQueryBuilder().setContext(contextPrefix).
      allowEmptyMetric().build(-1, -1);
    metrics.addAll(table.getAllMetrics(query));
    return metrics;
  }
}
