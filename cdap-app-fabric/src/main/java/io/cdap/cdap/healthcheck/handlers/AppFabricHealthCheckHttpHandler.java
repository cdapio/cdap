/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.healthcheck.handlers;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.healthcheck.implementation.AppFabricHealthCheckImplementation;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * App Fabric Health Check HTTP Handler.
 */
@Liveness
@ApplicationScoped
@Path(Constants.Gateway.API_VERSION_3)
public class AppFabricHealthCheckHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricHealthCheckHttpHandler.class);
  private static final Gson GSON = new Gson();
  private final AppFabricHealthCheckImplementation appFabricHealthCheckImplementation;

  @Inject
  AppFabricHealthCheckHttpHandler(AppFabricHealthCheckImplementation appFabricHealthCheckImplementation) {
    this.appFabricHealthCheckImplementation = appFabricHealthCheckImplementation;
  }

  @GET
  @Path("/appfabric/health")
  public void call(HttpRequest request, HttpResponder responder) {
    HealthCheckResponse healthCheckResponse = appFabricHealthCheckImplementation.collect();
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(healthCheckResponse.getData()));
  }
}
