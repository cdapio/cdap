/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.implementation.HealthCheckImplementation;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * App Fabric Health Check HTTP Handler.
 */
@Liveness
@ApplicationScoped
@Path(Constants.Gateway.API_VERSION_3)
public class HealthCheckHttpHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HealthCheckHttpHandler.class);
  private static final Gson GSON = new Gson();
  private final HealthCheckImplementation healthCheckImplementation;

  @Inject
  HealthCheckHttpHandler(HealthCheckImplementation appFabricHealthCheckImplementation) {
    this.healthCheckImplementation = appFabricHealthCheckImplementation;
  }

  @GET
  @Path("/{namespace}/health")
  public void call(HttpRequest request, HttpResponder responder, @PathParam("namespace") String namespace,
                   @Nullable @QueryParam("podLabelSelector") String podLabelSelector,
                   @Nullable @QueryParam("nodeFieldSelector") String nodeFieldSelector) {
    HealthCheckResponse healthCheckResponse =
      healthCheckImplementation.collect(namespace, podLabelSelector, nodeFieldSelector);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(healthCheckResponse.getData()));
  }
}
