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

import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.ServerException;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.http.HandlerContext;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Handlers for clearing metrics.
 */
// todo: expire metrics where possible instead of explicit delete: CDAP-1124
@Path(Constants.Gateway.API_VERSION_2 + "/metrics")
public class DeleteMetricsHandler extends AuthenticatedHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteMetricsHandler.class);

  private final MetricStore metricStore;

  @Inject
  public DeleteMetricsHandler(Authenticator authenticator,
                              final MetricStore metricStore) {
    super(authenticator);

    this.metricStore = metricStore;
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
    handleDelete(request, responder, metricPrefix);
  }

  @DELETE
  @Path("/{scope}")
  public void deleteScope(HttpRequest request, HttpResponder responder,
                          @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    handleDelete(request, responder, metricPrefix);
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
      URI uri = new URI(MetricQueryParser.stripVersionAndMetricsFromPath(request.getUri()));
      MetricDeleteQuery query = MetricQueryParser.parseDelete(uri, metricPrefix);
      metricStore.delete(query);
      responder.sendJson(HttpResponseStatus.OK, "OK");
    } catch (URISyntaxException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (MetricsPathException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (ServerException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error while deleting metrics");
    } catch (Exception e) {
      LOG.error("Caught exception while deleting metrics {}", e.getMessage(), e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error while deleting metrics");
    }
  }
}
