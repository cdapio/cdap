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
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.metrics.data.MetricsScanQuery;
import co.cask.cdap.metrics.data.MetricsScanQueryBuilder;
import co.cask.cdap.metrics.data.MetricsTableFactory;
import co.cask.cdap.metrics.data.TimeSeriesTable;
import co.cask.http.HttpResponder;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
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

  private final MetricsRequestExecutor requestExecutor;
  private final Supplier<TimeSeriesTable> timeSeriesTables;

  // NOTE: Hour is the lowest resolution and also has the highest TTL compared to minute and second resolutions.
  // highest TTL ensures, this is good for searching all available metrics and also has fewer rows of data points.
  private static final int LOWEST_RESOLUTION = 3600;

  @Inject
  public MetricsHandler(Authenticator authenticator,
                        final MetricsTableFactory metricsTableFactory) {
    super(authenticator);

    this.requestExecutor = new MetricsRequestExecutor(metricsTableFactory);

    this.timeSeriesTables = Suppliers.memoize(new Supplier<TimeSeriesTable>() {
      @Override
      public TimeSeriesTable get() {
        return metricsTableFactory.createTimeSeries(LOWEST_RESOLUTION);
      }
    });
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
    MetricsRequestBuilder builder =
      new MetricsRequestBuilder(null)
        .setContextPrefix(context)
        .setMetricPrefix(metric);
    // sets time range, query type, etc.
    MetricsRequestParser.parseQueryString(new URI(request.getUri()), builder);
    JsonElement queryResult = requestExecutor.executeQuery(builder.build());
    responder.sendJson(HttpResponseStatus.OK, queryResult);
  }

  private void searchMetricAndRespond(HttpResponder responder, String context) {
    try {
      responder.sendJson(HttpResponseStatus.OK, searchMetric(context));
    } catch (OperationException e) {
      LOG.warn("Exception while retrieving available metrics", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void searchChildContextAndRespond(HttpResponder responder, String context) {
    try {
      responder.sendJson(HttpResponseStatus.OK, searchChildContext(context));
    } catch (OperationException e) {
      LOG.warn("Exception while retrieving contexts", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private Collection<String> searchChildContext(String contextPrefix) throws OperationException {
    SortedSet<String> nextLevelContexts = Sets.newTreeSet();
    TimeSeriesTable table = timeSeriesTables.get();
    MetricsScanQuery query = new MetricsScanQueryBuilder().setContext(contextPrefix).
      allowEmptyMetric().build(-1, -1);

    List<String> results = table.getNextLevelContexts(query);
    for (String context : results) {
      int nextPartEnd = context.indexOf(".", contextPrefix == null ? 0 : contextPrefix.length() + 1);
      nextLevelContexts.add(nextPartEnd == -1 ? context : context.substring(0, nextPartEnd));
    }
    return nextLevelContexts;
  }

  private Set<String> searchMetric(String contextPrefix) throws OperationException {
    SortedSet<String> metrics = Sets.newTreeSet();
    TimeSeriesTable table = timeSeriesTables.get();
    MetricsScanQuery query = new MetricsScanQueryBuilder().setContext(contextPrefix).
      allowEmptyMetric().build(-1, -1);
    metrics.addAll(table.getAllMetrics(query));
    return metrics;
  }
}
