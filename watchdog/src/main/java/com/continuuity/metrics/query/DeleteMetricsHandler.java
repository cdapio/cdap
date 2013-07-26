package com.continuuity.metrics.query;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.metrics.data.AggregatesTable;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.data.TimeSeriesTable;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.IOException;

/**
 * Handlers for clearing metrics.
 */
@Path("/metrics")
public class DeleteMetricsHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteMetricsHandler.class);
  private final LoadingCache<Integer, TimeSeriesTable> metricsTableCache;
  private final AggregatesTable aggregatesTable;

  @Inject
  public DeleteMetricsHandler(final MetricsTableFactory metricsTableFactory) {
    this.metricsTableCache = CacheBuilder.newBuilder().build(new CacheLoader<Integer, TimeSeriesTable>() {
      @Override
      public TimeSeriesTable load(Integer key) throws Exception {
        return metricsTableFactory.createTimeSeries(MetricsScope.REACTOR.name(), key);
      }
    });
    this.aggregatesTable = metricsTableFactory.createAggregates(MetricsScope.REACTOR.name());
  }

  @Path("/app/{app-id}")
  @DELETE
  public void deleteAppMetrics(HttpRequest request, HttpResponder responder,
                               @PathParam("app-id") String appId) throws IOException {
    try {
      LOG.debug("Request to delete metrics for application {}", appId);
      metricsTableCache.getUnchecked(1).delete(appId);
      aggregatesTable.delete(appId);
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
      metricsTableCache.getUnchecked(1).clear();
      aggregatesTable.clear();
      responder.sendString(HttpResponseStatus.OK, "OK");
    } catch (OperationException e) {
      LOG.debug("Caught exception while deleting metrics {}", e.getMessage(), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error while deleting application");
    }
  }
}
