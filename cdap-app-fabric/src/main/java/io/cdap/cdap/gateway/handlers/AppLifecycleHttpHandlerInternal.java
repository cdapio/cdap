/*
 * Copyright © 2020 Cask Data, Inc.
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
import com.google.inject.Singleton;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 * Internal {@link HttpHandler} for Application Lifecycle Management
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/namespaces/{namespace-id}")
public class AppLifecycleHttpHandlerInternal extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new Gson();

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  private final CConfiguration configuration;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final NamespacePathLocator namespacePathLocator;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final File tmpDir;

  @Inject
  AppLifecycleHttpHandlerInternal(CConfiguration configuration,
                                  ProgramRuntimeService runtimeService,
                                  NamespaceQueryAdmin namespaceQueryAdmin,
                                  NamespacePathLocator namespacePathLocator,
                                  ApplicationLifecycleService applicationLifecycleService) {
    this.configuration = configuration;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.runtimeService = runtimeService;
    this.namespacePathLocator = namespacePathLocator;
    this.applicationLifecycleService = applicationLifecycleService;
    this.tmpDir = new File(new File(configuration.get(Constants.CFG_LOCAL_DATA_DIR)),
                           configuration.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  /**
   * Get a list of {@link ApplicationDetail} for all applications in the given namespace
   *
   * @param request   {@link HttpRequest}
   * @param responder {@link HttpResponse}
   * @param namespace the namespace to get all application details
   * @throws Exception if namespace doesn't exists or failed to get all application details
   */
  @GET
  @Path("/apps")
  public void getAllAppDetails(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespace) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(applicationLifecycleService.getApps(namespaceId, detail -> true)));
  }

  /**
   * Get {@link ApplicationDetail} for a given application
   *
   * @param request     {@link HttpRequest}
   * @param responder   {@link HttpResponse}
   * @param namespace   the namespace to get all application details   *
   * @param application the id of the application to get its {@link ApplicationDetail}
   * @throws Exception if either namespace or application doesn't exist, or failed to get {@link ApplicationDetail}
   */
  @GET
  @Path("/app/{app-id}")
  public void getAppDetail(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespace,
                           @PathParam("app-id") String application) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    ApplicationId appId = new ApplicationId(namespace, application);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(applicationLifecycleService.getAppDetail(appId)));
  }

  /**
   * Get {@link ApplicationDetail} for a given application
   *
   * @param request     {@link HttpRequest}
   * @param responder   {@link HttpResponse}
   * @param namespace   the namespace to get all application details
   * @param application the id of the application to get its {@link ApplicationDetail}
   * @throws Exception if either namespace or application doesn't exist, or failed to get {@link ApplicationDetail}
   */
  @GET
  @Path("/app/{app-id}/versions/{version-id}")
  public void getAppDetailForVersion(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") final String namespace,
                                     @PathParam("app-id") final String application,
                                     @PathParam("version-id") final String version) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    ApplicationId appId = new ApplicationId(namespace, application, version);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(applicationLifecycleService.getAppDetail(appId)));
  }
}
