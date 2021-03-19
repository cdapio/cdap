/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.store.NamespaceStore;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Program Preferences HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class PreferencesHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Gson GSON = new Gson();

  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final PreferencesService preferencesService;
  private final Store store;
  private final NamespaceStore nsStore;

  @Inject
  PreferencesHttpHandler(AuthorizationEnforcer authorizationEnforcer,
                         AuthenticationContext authenticationContext,
                         PreferencesService preferencesService,
                         Store store, NamespaceStore nsStore) {
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
    this.preferencesService = preferencesService;
    this.store = store;
    this.nsStore = nsStore;
  }

  //Instance Level Properties
  @Path("/preferences")
  @GET
  public void getInstancePrefs(HttpRequest request, HttpResponder responder) throws Exception {
    InstanceId instanceId = new InstanceId("");
    AuthorizationUtil.ensureAccess(instanceId, authorizationEnforcer, authenticationContext.getPrincipal());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(preferencesService.getProperties()));
  }

  @Path("/preferences")
  @DELETE
  public void deleteInstancePrefs(HttpRequest request, HttpResponder responder) throws Exception {
    InstanceId instanceId = new InstanceId("");
    authorizationEnforcer.enforce(instanceId, authenticationContext.getPrincipal(), Action.ADMIN);
    preferencesService.deleteProperties();
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/preferences")
  @PUT
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void setInstancePrefs(FullHttpRequest request, HttpResponder responder) throws Exception {
    InstanceId instanceId = new InstanceId("");
    authorizationEnforcer.enforce(instanceId, authenticationContext.getPrincipal(), Action.ADMIN);
    try {
      Map<String, String> propMap = decodeArguments(request);
      preferencesService.setProperties(propMap);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException jsonEx) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
    }
  }

  //Namespace Level Properties
  //Resolved field, if set to true, returns the collapsed property map (Instance < Namespace)
  @Path("/namespaces/{namespace-id}/preferences")
  @GET
  public void getNamespacePrefs(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespace, @QueryParam("resolved") boolean resolved)
    throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    AuthorizationUtil.ensureAccess(namespaceId, authorizationEnforcer, authenticationContext.getPrincipal());
    if (nsStore.get(namespaceId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not present", namespace));
    } else {
      if (resolved) {
        responder.sendJson(HttpResponseStatus.OK,
                           GSON.toJson(preferencesService.getResolvedProperties(namespaceId)));
      } else {
        responder.sendJson(HttpResponseStatus.OK,
                           GSON.toJson(preferencesService.getProperties(namespaceId)));
      }
    }
  }

  @Path("/namespaces/{namespace-id}/preferences")
  @PUT
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void setNamespacePrefs(FullHttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespace) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    authorizationEnforcer.enforce(namespaceId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (nsStore.get(namespaceId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not present", namespace));
      return;
    }

    try {
      Map<String, String> propMap = decodeArguments(request);
      preferencesService.setProperties(namespaceId, propMap);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException jsonEx) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
    }
  }

  @Path("/namespaces/{namespace-id}/preferences")
  @DELETE
  public void deleteNamespacePrefs(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespace) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    authorizationEnforcer.enforce(namespaceId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (nsStore.get(namespaceId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not present", namespace));
    } else {
      preferencesService.deleteProperties(namespaceId);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  //Application Level Properties
  //Resolved field, if set to true, returns the collapsed property map (Instance < Namespace < Application)
  @Path("/namespaces/{namespace-id}/apps/{application-id}/preferences")
  @GET
  public void getAppPrefs(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId,
                          @QueryParam("resolved") boolean resolved) throws Exception {
    ApplicationId applicationId = new ApplicationId(namespace, appId);
    AuthorizationUtil.ensureAccess(applicationId, authorizationEnforcer, authenticationContext.getPrincipal());
    if (store.getApplication(applicationId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Application %s in Namespace %s not present",
                                                                       appId, namespace));
    } else {
      if (resolved) {
        responder.sendJson(HttpResponseStatus.OK, GSON.toJson(preferencesService.getResolvedProperties(applicationId)));
      } else {
        responder.sendJson(HttpResponseStatus.OK, GSON.toJson(preferencesService.getProperties(applicationId)));
      }
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{application-id}/preferences")
  @PUT
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void putAppPrefs(FullHttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId)
    throws Exception {
    ApplicationId applicationId = new ApplicationId(namespace, appId);
    authorizationEnforcer.enforce(applicationId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (store.getApplication(applicationId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Application %s in Namespace %s not present",
                                                                       appId, namespace));
      return;
    }

    try {
      Map<String, String> propMap = decodeArguments(request);
      preferencesService.setProperties(applicationId, propMap);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException jsonEx) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{application-id}/preferences")
  @DELETE
  public void deleteAppPrefs(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId)
    throws Exception {
    ApplicationId applicationId = new ApplicationId(namespace, appId);
    authorizationEnforcer.enforce(applicationId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (store.getApplication(applicationId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Application %s in Namespace %s not present",
                                                                       appId, namespace));
    } else {
      preferencesService.deleteProperties(applicationId);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  //Program Level Properties
  //Resolved field, if set to true, returns the collapsed property map (Instance < Namespace < Application < Program)
  @Path("/namespaces/{namespace-id}/apps/{application-id}/{program-type}/{program-id}/preferences")
  @GET
  public void getProgramPrefs(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId,
                              @PathParam("program-type") String programType, @PathParam("program-id") String programId,
                              @QueryParam("resolved") boolean resolved) throws Exception {
    ProgramId program = new ProgramId(namespace, appId, getProgramType(programType), programId);
    authorizationEnforcer.enforce(program, authenticationContext.getPrincipal(), Action.ADMIN);
    Store.ensureProgramExists(program, store.getApplication(program.getParent()));
    if (resolved) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(preferencesService.getResolvedProperties(program)));
    } else {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(preferencesService.getProperties(program)));
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{application-id}/{program-type}/{program-id}/preferences")
  @PUT
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void putProgramPrefs(FullHttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespace,
                              @PathParam("application-id") String appId,
                              @PathParam("program-type") String programType,
                              @PathParam("program-id") String programId) throws Exception {
    ProgramId program = new ProgramId(namespace, appId, getProgramType(programType), programId);
    authorizationEnforcer.enforce(program, authenticationContext.getPrincipal(), Action.ADMIN);
    Store.ensureProgramExists(program, store.getApplication(program.getParent()));
    try {
      Map<String, String> propMap = decodeArguments(request);
      preferencesService.setProperties(program, propMap);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException jsonEx) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{application-id}/{program-type}/{program-id}/preferences")
  @DELETE
  public void deleteProgramPrefs(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId,
                                 @PathParam("program-type") String programType,
                                 @PathParam("program-id") String programId) throws Exception {
    ProgramId program = new ProgramId(namespace, appId, getProgramType(programType), programId);
    authorizationEnforcer.enforce(program, authenticationContext.getPrincipal(), Action.ADMIN);
    Store.ensureProgramExists(program, store.getApplication(program.getParent()));
    preferencesService.deleteProperties(program);
    responder.sendStatus(HttpResponseStatus.OK);
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
