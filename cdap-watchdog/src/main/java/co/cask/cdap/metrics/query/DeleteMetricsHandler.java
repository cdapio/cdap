/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.common.service.ServerException;
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.metrics.data.AggregatesTable;
import co.cask.cdap.metrics.data.MetricsTableFactory;
import co.cask.http.HandlerContext;
import co.cask.http.HttpResponder;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Handlers for clearing metrics.
 */
// todo: expire metrics where possible instead of explicit delete: CDAP-1124
@Path(Constants.Gateway.API_VERSION_2 + "/metrics")
public class DeleteMetricsHandler extends BaseMetricsHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteMetricsHandler.class);


  private final Supplier<Map<MetricsScope, AggregatesTable>> aggregatesTables;

  @Inject
  public DeleteMetricsHandler(Authenticator authenticator,
                              final MetricsTableFactory metricsTableFactory, CConfiguration cConf) {
    super(authenticator);
    
    this.aggregatesTables = Suppliers.memoize(new Supplier<Map<MetricsScope, AggregatesTable>>() {
      @Override
      public Map<MetricsScope, AggregatesTable> get() {
        Map<MetricsScope, AggregatesTable> map = Maps.newHashMap();
        for (final MetricsScope scope : MetricsScope.values()) {
          map.put(scope, metricsTableFactory.createAggregates(scope.name()));
        }
        return map;
      }
    });
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
  public void deleteAllMetrics(HttpRequest request, HttpResponder responder,
                               @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    try {
      if (metricPrefix == null) {
        LOG.debug("Request to delete all metrics");
      } else {
        LOG.debug("Request to delete all metrics that begin with entities {}", metricPrefix);
      }
      for (MetricsScope scope : MetricsScope.values()) {
        deleteTableEntries(scope, null, metricPrefix, null);
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
                          @PathParam("scope") String scope,
                          @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    try {
      if (metricPrefix == null) {
        LOG.debug("Request to delete all metrics in scope {} ", scope);
      } else {
        LOG.debug("Request to delete all metrics that begin with entities {} in scope {}", metricPrefix, scope);
      }
      MetricsScope metricsScope = null;
      try {
        metricsScope = MetricsScope.valueOf(scope.toUpperCase());
      } catch (IllegalArgumentException e) {
        responder.sendError(HttpResponseStatus.NOT_FOUND, "scope " + scope + " not found.");
        return;
      }
      deleteTableEntries(metricsScope, null, metricPrefix, null);
      responder.sendString(HttpResponseStatus.OK, "OK");
    } catch (OperationException e) {
      LOG.error("Caught exception while deleting metrics {}", e.getMessage(), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error while deleting " + scope + " metrics");
    }
  }

  // ex: /system/apps/appX, /system/streams/streamX, /system/dataset/datasetX
  @DELETE
  @Path("/{scope}/{type}/{type-id}")
  public void deleteType(HttpRequest request, HttpResponder responder,
                         @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    handleDelete(request, responder, metricPrefix);
  }

  // ex: /system/apps/appX/flows
  @DELETE
  @Path("/{scope}/{type}/{type-id}/{program-type}")
  public void deleteProgramType(HttpRequest request, HttpResponder responder,
                                @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    handleDelete(request, responder, metricPrefix);
  }

  // ex: /system/apps/appX/flows/flowY
  @DELETE
  @Path("/{scope}/{type}/{type-id}/{program-type}/{program-id}")
  public void deleteProgram(HttpRequest request, HttpResponder responder,
                            @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    handleDelete(request, responder, metricPrefix);
  }

  // ex: /system/apps/appX/mapreduce/jobId/mappers
  @DELETE
  @Path("/{scope}/{type}/{type-id}/{program-type}/{program-id}/{component-type}")
  public void handleComponentType(HttpRequest request, HttpResponder responder,
                                  @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    handleDelete(request, responder, metricPrefix);
  }

  // ex: /system/apps/appX/flows/flowY/flowlets/flowletZ
  @DELETE
  @Path("/{scope}/{type}/{type-id}/{program-type}/{program-id}/{component-type}/{component-id}")
  public void deleteComponent(HttpRequest request, HttpResponder responder,
                              @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    handleDelete(request, responder, metricPrefix);
  }

  // ex: /system/datasets/tickTimeseries/apps/Ticker/flows/TickerTimeseriesFlow/flowlets/saver
  @DELETE
  @Path("/system/datasets/{dataset-id}/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}")
  public void deleteFlowletDatasetMetrics(HttpRequest request, HttpResponder responder,
                                          @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    handleDelete(request, responder, metricPrefix);
  }

  private void handleDelete(HttpRequest request, HttpResponder responder, String metricPrefix) {
    try {
      URI uri = new URI(MetricsRequestParser.stripVersionAndMetricsFromPath(request.getUri()));
      MetricsRequestBuilder requestBuilder = new MetricsRequestBuilder(uri);
      MetricsRequestContext metricsRequestContext = MetricsRequestParser.parseContext(uri.getPath(), requestBuilder);
      this.validatePathElements(request, metricsRequestContext);
      MetricsRequest metricsRequest = requestBuilder.build();

      deleteTableEntries(metricsRequest.getScope(), metricsRequest.getContextPrefix(),
                         metricPrefix, metricsRequest.getTagPrefix());
      responder.sendJson(HttpResponseStatus.OK, "OK");
    } catch (URISyntaxException e) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (OperationException e) {
      LOG.error("Caught exception while deleting metrics {}", e.getMessage(), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error while deleting metrics");
    } catch (MetricsPathException e) {
      responder.sendError(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (ServerException e) {
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error while deleting metrics");
    }
  }

  private void deleteTableEntries(MetricsScope scope, String contextPrefix,
                                  String metricPrefix, String tag) throws OperationException {
    AggregatesTable aggTable = aggregatesTables.get().get(scope);

    if (contextPrefix == null && tag == null && metricPrefix == null) {
      aggTable.clear();
    } else if (tag == null) {
      aggTable.delete(contextPrefix, metricPrefix);
    } else {
      aggTable.delete(contextPrefix, metricPrefix, "0", tag);
    }
  }
}
