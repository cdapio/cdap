package com.continuuity.metrics.query;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.metrics.data.AggregatesTable;
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
import java.util.Map;

/**
 * Handlers for clearing metrics.
 */
@Path("/metrics")
public class DeleteMetricsHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteMetricsHandler.class);

  private final Map<MetricsScope, LoadingCache<Integer, TimeSeriesTable>> metricsTableCaches;
  private final Map<MetricsScope, AggregatesTable> aggregatesTables;

  @Inject
  public DeleteMetricsHandler(final MetricsTableFactory metricsTableFactory) {
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

  @Path("/app/{app-id}")
  @DELETE
  public void deleteAppMetrics(HttpRequest request, HttpResponder responder,
                               @PathParam("app-id") String appId) throws IOException {
    try {
      LOG.debug("Request to delete metrics for application {}", appId);
      for (LoadingCache<Integer, TimeSeriesTable> metricsTableCache : metricsTableCaches.values()) {
        metricsTableCache.getUnchecked(1).delete(appId);
      }
      for (AggregatesTable aggregatesTable : aggregatesTables.values()) {
        aggregatesTable.delete(appId);
      }
      responder.sendString(HttpResponseStatus.OK, "OK");
    } catch (OperationException e) {
      LOG.debug("Caught exception while deleting metrics {}", e.getMessage(), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error while deleting application");
    }
  }

  @DELETE
  public void deleteAllMetrics(HttpRequest request, HttpResponder responder) throws IOException{
    try {
      LOG.debug("Request to delete metrics all");
      for (LoadingCache<Integer, TimeSeriesTable> metricsTableCache : metricsTableCaches.values()) {
        metricsTableCache.getUnchecked(1).clear();
      }
      for (AggregatesTable aggregatesTable : aggregatesTables.values()) {
        aggregatesTable.clear();
      }
      responder.sendString(HttpResponseStatus.OK, "OK");
    } catch (OperationException e) {
      LOG.debug("Caught exception while deleting metrics {}", e.getMessage(), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error while deleting application");
    }
  }
}
