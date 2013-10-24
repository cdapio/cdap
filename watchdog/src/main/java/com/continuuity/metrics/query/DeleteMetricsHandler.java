package com.continuuity.metrics.query;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data2.OperationException;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.data.AggregatesTable;
import com.continuuity.metrics.data.MetricsScanQuery;
import com.continuuity.metrics.data.MetricsScanQueryBuilder;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.data.TimeSeriesTable;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Handlers for clearing metrics.
 */
@Path(Constants.Gateway.GATEWAY_VERSION + "/metrics")
public class DeleteMetricsHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteMetricsHandler.class);

  private final Map<MetricsScope, LoadingCache<Integer, TimeSeriesTable>> metricsTableCaches;
  private final Map<MetricsScope, AggregatesTable> aggregatesTables;
  private final int tsRetentionSeconds;

  @Inject
  public DeleteMetricsHandler(final MetricsTableFactory metricsTableFactory, CConfiguration cConf) {
    this.metricsTableCaches = Maps.newHashMap();
    this.aggregatesTables = Maps.newHashMap();
    for (final MetricsScope scope : MetricsScope.values()) {
      LoadingCache<Integer, TimeSeriesTable> cache =
        CacheBuilder.newBuilder().build(new CacheLoader<Integer, TimeSeriesTable>() {
          @Override
          public TimeSeriesTable load(Integer key) throws Exception {
            return metricsTableFactory.createTimeSeries(scope.name(), key);
          }
        });
      this.metricsTableCaches.put(scope, cache);
      this.aggregatesTables.put(scope, metricsTableFactory.createAggregates(scope.name()));
    }

    String retentionStr = cConf.get(MetricsConstants.ConfigKeys.RETENTION_SECONDS);
    this.tsRetentionSeconds = (retentionStr == null) ?
      (int) TimeUnit.SECONDS.convert(MetricsConstants.DEFAULT_RETENTION_HOURS, TimeUnit.HOURS) :
      Integer.parseInt(retentionStr);
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    LOG.info("Starting DeleteMetricsHandler");
  }

  @Override
  public void destroy(HandlerContext context) {
    super.destroy(context);
    LOG.info("Stopping DeleteMetricsHandler");
  }

  @DELETE
  public void deleteAllMetrics(HttpRequest request, HttpResponder responder) throws IOException{
    try {
      LOG.debug("Request to delete all metrics");
      for (MetricsScope scope : MetricsScope.values()) {
        deleteTableEntries(scope);
      }
      responder.sendString(HttpResponseStatus.OK, "OK");
    } catch (OperationException e) {
      LOG.error("Caught exception while deleting metrics {}", e.getMessage(), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error while deleting all metrics");
    }
  }

  @DELETE
  @Path("/{scope}")
  public void deleteScope(HttpRequest request, HttpResponder responder,
                          @PathParam("scope") String scope) throws IOException{
    try {
      LOG.debug("Request to delete all metrics in scope {} ", scope);
      MetricsScope metricsScope = MetricsScope.valueOf(scope.toUpperCase());
      deleteTableEntries(metricsScope);
      responder.sendString(HttpResponseStatus.OK, "OK");
    } catch (OperationException e) {
      LOG.error("Caught exception while deleting metrics {}", e.getMessage(), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error while deleting " + scope + " metrics");
    }
  }

  // ex: /reactor/apps/appX, /reactor/streams/streamX, /reactor/dataset/datasetX
  @DELETE
  @Path("/{scope}/{type}/{type-id}")
  public void deleteType(HttpRequest request, HttpResponder responder) throws IOException, OperationException {
    handleDelete(request, responder);
  }

  // ex: /reactor/apps/appX/flows
  @DELETE
  @Path("/{scope}/{type}/{type-id}/{program-type}")
  public void deleteProgramType(HttpRequest request, HttpResponder responder) throws IOException, OperationException {
    handleDelete(request, responder);
  }

  // ex: /reactor/apps/appX/flows/flowY
  @DELETE
  @Path("/{scope}/{type}/{type-id}/{program-type}/{program-id}")
  public void deleteProgram(HttpRequest request, HttpResponder responder) throws IOException, OperationException {
    handleDelete(request, responder);
  }

  // ex: /reactor/apps/appX/mapreduce/jobId/mappers
  @DELETE
  @Path("/{scope}/{type}/{type-id}/{program-type}/{program-id}/{component-type}")
  public void handleComponentType(HttpRequest request, HttpResponder responder) throws IOException, OperationException {
    handleDelete(request, responder);
  }

  // ex: /reactor/apps/appX/flows/flowY/flowlets/flowletZ
  @DELETE
  @Path("/{scope}/{type}/{type-id}/{program-type}/{program-id}/{component-type}/{component-id}")
  public void deleteComponent(HttpRequest request, HttpResponder responder) throws IOException, OperationException {
    handleDelete(request, responder);
  }

  private void handleDelete(HttpRequest request, HttpResponder responder) throws IOException, OperationException {
    try {
      URI uri = new URI(MetricsRequestParser.stripVersionAndMetricsFromPath(request.getUri()));
      MetricsRequestBuilder requestBuilder = new MetricsRequestBuilder(uri);
      MetricsRequestParser.parseContext(uri.getPath(), requestBuilder);
      MetricsRequest metricsRequest = requestBuilder.build();
      deleteTableEntries(metricsRequest.getScope(), metricsRequest.getContextPrefix(), metricsRequest.getTagPrefix());
      responder.sendJson(HttpResponseStatus.OK, "OK");
    } catch (URISyntaxException e) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (OperationException e) {
      LOG.error("Caught exception while deleting metrics {}", e.getMessage(), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error while deleting metrics");
    }
  }

  private void deleteTableEntries(MetricsScope scope) throws OperationException {
    deleteTableEntries(scope, null);
  }

  private void deleteTableEntries(MetricsScope scope, String contextPrefix) throws OperationException {
    deleteTableEntries(scope, contextPrefix, null);
  }

  private void deleteTableEntries(MetricsScope scope, String contextPrefix, String tag) throws OperationException {
    TimeSeriesTable ts1Table = metricsTableCaches.get(scope).getUnchecked(1);
    AggregatesTable aggTable = aggregatesTables.get(scope);

    if (contextPrefix == null && tag == null) {
      ts1Table.clear();
      aggTable.clear();
    } else if (tag == null) {
      ts1Table.delete(contextPrefix);
      aggTable.delete(contextPrefix);
    } else {
      long now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      MetricsScanQuery scanQuery = new MetricsScanQueryBuilder()
        .setContext(contextPrefix)
        .setMetric(null)
        .allowEmptyMetric()
        .setRunId("0")
        .setTag(tag)
        .build(now - tsRetentionSeconds, now + 10);
      ts1Table.delete(scanQuery);
      aggTable.delete(contextPrefix, null, "0", tag);
    }
  }
}
