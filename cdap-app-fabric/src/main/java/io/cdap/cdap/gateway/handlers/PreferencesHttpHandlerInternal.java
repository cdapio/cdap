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
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Program Preferences HTTP Handler for internal usage
 */
@Path(Constants.Gateway.INTERNAL_API_VERSION_3)
public class PreferencesHttpHandlerInternal extends AbstractAppFabricHttpHandler {

  private static final Gson GSON = new Gson();

  private final PreferencesService preferencesService;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  PreferencesHttpHandlerInternal(PreferencesService preferencesService,
                                 ApplicationLifecycleService applicationLifecycleService,
                                 NamespaceQueryAdmin namespaceQueryAdmin) {
    this.preferencesService = preferencesService;
    this.applicationLifecycleService = applicationLifecycleService;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  /**
   * Get instance level preferences
   *
   * @param request {@link HttpRequest}
   * @param responder the responder used for sending response back to client
   */
  @Path("/preferences")
  @GET
  public void getInstancePreferences(HttpRequest request, HttpResponder responder) {
    PreferencesDetail detail = preferencesService.getPreferences();
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(detail, PreferencesDetail.class));
  }

  /**
   * Get namespace level preferences.
   *
   * Note that if the given namespace doesn't exist, the return {@link PreferencesDetail} will be empty
   * (i.e. {@link PreferencesDetail#properties} will be an empty map). In the case of requesting resolved preferences,
   * the returned {@link PreferencesDetail} will include preferences from ancestor (i.e. preferences at instance level)
   *
   * @param request {@link HttpRequest}
   * @param responder the responder used for sending response back to client
   * @param namespace the namespace to get preferences for
   * @param resolved whether to return resolved preferences or not
   */
  @Path("/namespaces/{namespace-id}/preferences")
  @GET
  public void getNamespacePreferences(HttpRequest request, HttpResponder responder,
                                      @PathParam("namespace-id") String namespace,
                                      @QueryParam("resolved") boolean resolved) {
    NamespaceId namespaceId = new NamespaceId(namespace);
    // No need to check if namespace exists. PreferencesService returns an empty PreferencesDetail when that happens.
    PreferencesDetail detail;
    if (resolved) {
      detail = preferencesService.getResolvedPreferences(namespaceId);
    } else {
      detail = preferencesService.getPreferences(namespaceId);
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(detail, PreferencesDetail.class));
  }

  /**
   * Get application level preferences
   *
   * Note that if the given application doesn't exist, the return {@link PreferencesDetail} will be empty
   * (i.e. {@link PreferencesDetail#properties} will be an empty map). In the case of requesting resolved preferences,
   * the returned {@link PreferencesDetail} will include preferences from ancestor (i.e. preferences at namespace
   * and instance level)
   *
   * @param request {@link HttpRequest}
   * @param responder the responder used for sending response back to client
   * @param namespace the namespace of the application
   * @param appId the application to get preferences for
   * @param resolved whether to return resolved preferences or not
   */
  @Path("/namespaces/{namespace-id}/apps/{application-id}/preferences")
  @GET
  public void getApplicationPreferences(HttpRequest request, HttpResponder responder,
                                        @PathParam("namespace-id") String namespace,
                                        @PathParam("application-id") String appId,
                                        @QueryParam("resolved") boolean resolved) {
    ApplicationId applicationId = new ApplicationId(namespace, appId);
    // No need to check if application exists. PreferencesService returns an empty PreferencesDetail when that happens.
    PreferencesDetail detail;
    if (resolved) {
      detail = preferencesService.getResolvedPreferences(applicationId);
    } else {
      detail = preferencesService.getPreferences(applicationId);
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(detail, PreferencesDetail.class));
  }

  /**
   * Get program level preferences
   *
   * Note that if the given program doesn't exist, the return {@link PreferencesDetail} will be empty
   * (i.e. {@link PreferencesDetail#properties} will be an empty map). In the case of requesting resolved preferences,
   * the returned {@link PreferencesDetail} will include preferences from ancestor (i.e. preferences at application,
   * namespace and instance level)
   *
   * @param request {@link HttpRequest}
   * @param responder the responder used for sending response back to client
   * @param namespace the namespace of the application
   * @param appId the application to get preferences for
   * @param programType the type of the program
   * @param programId id of the program to get preferences for
   * @param resolved whether to return resolved preferences or not
   * @throws Exception
   */
  @Path("/namespaces/{namespace-id}/apps/{application-id}/{program-type}/{program-id}/preferences")
  @GET
  public void getProgramPreferences(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespace,
                                    @PathParam("application-id") String appId,
                                    @PathParam("program-type") String programType,
                                    @PathParam("program-id") String programId,
                                    @QueryParam("resolved") boolean resolved) throws Exception {
    ProgramId program = new ProgramId(namespace, appId, getProgramType(programType), programId);
    // No need to check if program exists. PreferencesService returns an empty PreferencesDetail when that happens.
    PreferencesDetail detail;
    if (resolved) {
      detail = preferencesService.getResolvedPreferences(program);
    } else {
      detail = preferencesService.getPreferences(program);
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(detail, PreferencesDetail.class));
  }

  /**
   * Parses the give program type into {@link ProgramType} object.
   *
   * @param programType the program type to parse.
   * @throws BadRequestException if the given program type is not a valid {@link ProgramType}.
   */
  private ProgramType getProgramType(String programType) throws BadRequestException {
    try {
      return ProgramType.valueOfCategoryName(programType);
    } catch (Exception e) {
      throw new BadRequestException(String.format("Invalid program type '%s'", programType), e);
    }
  }
}
