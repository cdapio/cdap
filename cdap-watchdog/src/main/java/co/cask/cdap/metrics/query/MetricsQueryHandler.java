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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.metrics.store.MetricStore;
import co.cask.cdap.metrics.store.cube.CubeQuery;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
/**
 * Class for handling requests for a single metric in a context.
 */
@Path(Constants.Gateway.API_VERSION_2 + "/metrics")
public class MetricsQueryHandler extends AuthenticatedHttpHandler {

  private final MetricStoreRequestExecutor requestExecutor;

  @Inject
  public MetricsQueryHandler(Authenticator authenticator, MetricStore metricStore) {
    super(authenticator);
    this.requestExecutor = new MetricStoreRequestExecutor(metricStore);
  }

  @GET
  @Path("/{scope}/{metric}")
  public void handleOverview(HttpRequest request, HttpResponder responder) throws IOException {
    handleRequest(request, responder);
  }

  @GET
  @Path("/system/cluster/{metric}")
  public void handleClusterMetrics(HttpRequest request, HttpResponder responder) throws IOException {
    handleRequest(request, responder);
  }

  // ex: /system/apps/appX/process.events.processed
  @GET
  @Path("/{scope}/{type}/{type-id}/{metric}")
  public void handleTopLevel(HttpRequest request, HttpResponder responder) throws IOException {
    handleRequest(request, responder);
  }

  // ex: /system/apps/appX/flows/process.events.processed
  @GET
  @Path("/{scope}/{type}/{type-id}/{request-type}/{metric}")
  public void handleProgramType(HttpRequest request, HttpResponder responder) throws IOException {
    handleRequest(request, responder);
  }

  // ex: /system/apps/appX/flows/flowY/process.events.processed
  @GET
  @Path("/{scope}/{type}/{type-id}/{request-type}/{request-id}/{metric}")
  public void handleProgram(HttpRequest request, HttpResponder responder) throws IOException {
    handleRequest(request, responder);
  }

  // ex: /system/apps/appX/mapreduce/jobId/mappers/process.entries.in
  @GET
  @Path("/{scope}/{type}/{type-id}/{request-type}/{request-id}/{component-type}/{metric}")
  public void handleComponentType(HttpRequest request, HttpResponder responder) throws IOException {
    handleRequest(request, responder);
  }

  // ex: /system/apps/appX/flows/flowY/flowlets/flowletZ/process.events.processed
  // ex2: /system/services/{service-name}/handlers/{handler-name}/methods/{method-name}/{metric}
  // ex3: /user/apps/appx/flows/flowZ/runs/897e3c92-f369-43de-94b1-7344ccf2fd13/events.sent
  @GET
  @Path("/{scope}/{type}/{type-id}/{request-type}/{request-id}/{component-type}/{component-id}/{metric}")
  public void handleComponent(HttpRequest request, HttpResponder responder) throws IOException {
    handleRequest(request, responder);
  }


  // ex: /system/apps/appX/mapreduce/jobId/runs/897e3c92-f369-43de-94b1-7344ccf2fd13/mappers/process.entries.in
  @GET
  @Path("/{scope}/{type}/{type-id}/{request-type}/{request-id}/runs/{run-id}/{component-type}/{metric}")
  public void handleComponentTypeWithRunId(HttpRequest request, HttpResponder responder) throws IOException {
    handleRequest(request, responder);
  }


  // ex: /system/apps/appX/flows/fowId/runs/897e3c92-f369-43de-94b1-7344ccf2fd13/flowlets/flowletId/process.entries.in
  @GET
  @Path("/{scope}/{type}/{type-id}/{request-type}/{request-id}/runs/{run-id}/" +
    "{component-type}/{component-id}/{metric}") //10
  public void handleComponentWithRunId(HttpRequest request, HttpResponder responder) throws IOException {
    handleRequest(request, responder);
  }

  // ex: /system/datasets/tickTimeseries/apps/Ticker/flows/TickerTimeseriesFlow/flowlets/saver/store.bytes
  @GET
  @Path("/system/datasets/{dataset-id}/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/{metric}")
  public void handleFlowletDatasetMetrics(HttpRequest request, HttpResponder responder)
    throws IOException {
    handleRequest(request, responder);
  }

  // ex: /system/datasets/tickTimeseries/apps/Ticker/flows/runs/897e3../TickerTimeseriesFlow/flowlets/saver/store.bytes
  @GET
  @Path("/system/datasets/{dataset-id}/apps/{app-id}/flows/{flow-id}/runs/{run-id}/flowlets/{flowlet-id}/{metric}")
  public void handleFlowletDatasetMetricsWithRunId(HttpRequest request, HttpResponder responder)
    throws IOException {
    handleRequest(request, responder);
  }

  @GET
  @Path("/system/transactions/{metric}")
  public void handleTransactionMetrics(HttpRequest request, HttpResponder response) throws IOException {
    handleRequest(request, response);
  }

  private void handleRequest(HttpRequest request, HttpResponder responder) throws IOException {
    try {
      URI uri = new URI(MetricQueryParser.stripVersionAndMetricsFromPath(request.getUri()));
      CubeQuery query = MetricQueryParser.parse(uri);
      responder.sendJson(HttpResponseStatus.OK, requestExecutor.executeQuery(query));
    } catch (URISyntaxException e) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (MetricsPathException e) {
      responder.sendError(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (Exception e) {
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying metrics");
    }
  }
}
