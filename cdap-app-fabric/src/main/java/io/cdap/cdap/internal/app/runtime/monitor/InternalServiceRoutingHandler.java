/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * The http handler for routing CDAP service requests from workers running in
 * the same cluster. This service delegates all auth checks to downstream
 * services.
 */
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/router")
public class InternalServiceRoutingHandler extends
    AbstractServiceRoutingHandler {

  @Inject
  InternalServiceRoutingHandler(DiscoveryServiceClient discoveryServiceClient) {
    super(discoveryServiceClient);
  }

  /**
   * Handles http calls without a request body. It forwards calls from program
   * runtime to access CDAP services.
   */
  @Path("/services/{service}/**")
  @GET
  @DELETE
  public void routeService(HttpRequest request, HttpResponder responder,
      @PathParam("service") String service) throws Exception {
    routeService(request, responder, service, getServicePath(request, service));
  }

  /**
   * Handles http calls with request body. It streams the body to the internal
   * service and forwards the response.
   */
  @Path("/services/{service}/**")
  @PUT
  @POST
  public BodyConsumer routeServiceWithBody(HttpRequest request,
      HttpResponder responder,
      @PathParam("service") String service) throws Exception {
    return routeServiceWithBody(request, responder, service,
        getServicePath(request, service));
  }

  private String getServicePath(HttpRequest request, String service) {
    String prefix = String.format(
        "%s/router/services/%s",
        Constants.Gateway.INTERNAL_API_VERSION_3, service);
    return request.uri().substring(prefix.length());
  }
}
