/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.config.PreferencesWrapper;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.HttpResponder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@Path(Constants.Gateway.API_VERSION_3 + "/configuration/preferences")
public class PreferencesHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PreferencesHttpHandler.class);

  private final PreferencesWrapper preferencesWrapper;
  private final Store store;

  @Inject
  public PreferencesHttpHandler(Authenticator authenticator, PreferencesWrapper preferencesWrapper,
                                StoreFactory storeFactory) {
    super(authenticator);
    this.preferencesWrapper = preferencesWrapper;
    this.store = storeFactory.create();
  }

  //Instance Level Properties
  @Path("/")
  @GET
  public void getInstance(final HttpRequest request, final HttpResponder responder) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, preferencesWrapper.getProperties());
  }

  @Path("/")
  @DELETE
  public void deleteInstance(final HttpRequest request, final HttpResponder responder) throws Exception {
    preferencesWrapper.deleteProperties();
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/")
  @PUT
  public void setInstance(final HttpRequest request, final HttpResponder responder) throws Exception {
    try {
      Map<String, String> propMap = decodeArguments(request);
      preferencesWrapper.setProperties(propMap);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException jsonEx) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
    }
  }

  //Namespace Level Properties
  @Path("/namespaces/{namespace-id}")
  @GET
  public void getNamespace(final HttpRequest request, final HttpResponder responder,
                           @PathParam("namespace-id") String namespace, @QueryParam("resolved") String resolved)
    throws Exception {
    if (store.getNamespace(Id.Namespace.from(namespace)) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not present", namespace));
    } else {
      if (resolved != null && resolved.equals("true")) {
        responder.sendJson(HttpResponseStatus.OK, preferencesWrapper.getResolvedProperties(namespace));
      } else {
        responder.sendJson(HttpResponseStatus.OK, preferencesWrapper.getProperties(namespace));
      }
    }
  }

  @Path("/namespaces/{namespace-id}")
  @PUT
  public void setNamespace(final HttpRequest request, final HttpResponder responder,
                           @PathParam("namespace-id") String namespace) throws Exception {
    if (store.getNamespace(Id.Namespace.from(namespace)) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not present", namespace));
      return;
    }

    try {
      Map<String, String> propMap = decodeArguments(request);
      preferencesWrapper.setProperties(namespace, propMap);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException jsonEx) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
    }
  }

  @Path("/namespaces/{namespace-id}")
  @DELETE
  public void deleteNamespace(final HttpRequest request, final HttpResponder responder,
                              @PathParam("namespace-id") String namespace) throws Exception {
    if (store.getNamespace(Id.Namespace.from(namespace)) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not present", namespace));
    } else {
      preferencesWrapper.deleteProperties(namespace);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  //Application Level Properties
  @Path("/namespaces/{namespace-id}/apps/{application-id}")
  @GET
  public void getApplication(final HttpRequest request, final HttpResponder responder,
                             @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId,
                             @QueryParam("resolved") String resolved) throws Exception {
    if (store.getApplication(Id.Application.from(namespace, appId)) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Application %s in Namespace %s not present",
                                                                       appId, namespace));
    } else {
      if (resolved != null && resolved.equals("true")) {
        responder.sendJson(HttpResponseStatus.OK, preferencesWrapper.getResolvedProperties(namespace, appId));
      } else {
        responder.sendJson(HttpResponseStatus.OK, preferencesWrapper.getProperties(namespace, appId));
      }
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{application-id}")
  @PUT
  public void putApplication(final HttpRequest request, final HttpResponder responder,
                             @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId)
    throws Exception {
    if (store.getApplication(Id.Application.from(namespace, appId)) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Application %s in Namespace %s not present",
                                                                       appId, namespace));
      return;
    }

    try {
      Map<String, String> propMap = decodeArguments(request);
      preferencesWrapper.setProperties(namespace, appId, propMap);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException jsonEx) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{application-id}")
  @DELETE
  public void deleteApplication(final HttpRequest request, final HttpResponder responder,
                                @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId)
    throws Exception {
    if (store.getApplication(Id.Application.from(namespace, appId)) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Application %s in Namespace %s not present",
                                                                       appId, namespace));
    } else {
      preferencesWrapper.deleteProperties(namespace, appId);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  //Program Level Properties
  @Path("/namespaces/{namespace-id}/apps/{application-id}/{program-type}/{program-id}")
  @GET
  public void getProgram(final HttpRequest request, final HttpResponder responder,
                         @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId,
                         @PathParam("program-type") String programType, @PathParam("program-id") String programId,
                         @QueryParam("resolved") String resolved) throws Exception {
    if (checkIfProgramExists(namespace, appId, programType, programId, responder)) {
      if (resolved != null && resolved.equals("true")) {
        responder.sendJson(HttpResponseStatus.OK, preferencesWrapper.getResolvedProperties(
          namespace, appId, programType, programId));
      } else {
        responder.sendJson(HttpResponseStatus.OK, preferencesWrapper.getProperties(
          namespace, appId, programType, programId));
      }
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{application-id}/{program-type}/{program-id}")
  @PUT
  public void putProgram(final HttpRequest request, final HttpResponder responder,
                         @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId,
                         @PathParam("program-type") String programType, @PathParam("program-id") String programId)
    throws Exception {
    if (checkIfProgramExists(namespace, appId, programType, programId, responder)) {
      try {
        Map<String, String> propMap = decodeArguments(request);
        preferencesWrapper.setProperties(namespace, appId, programType, programId, propMap);
        responder.sendStatus(HttpResponseStatus.OK);
      } catch (JsonSyntaxException jsonEx) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
      }
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{application-id}/{program-type}/{program-id}")
  @DELETE
  public void deleteProgram(final HttpRequest request, final HttpResponder responder,
                            @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId,
                            @PathParam("program-type") String programType, @PathParam("program-id") String programId)
    throws Exception {
    if (checkIfProgramExists(namespace, appId, programType, programId, responder)) {
      preferencesWrapper.deleteProperties(namespace, appId, programType, programId);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  private boolean checkIfProgramExists(String namespace, String appId, String programType, String programId,
                                       HttpResponder responder) throws Exception {
    ProgramType type;
    try {
      type = ProgramType.valueOfCategoryName(programType);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, String.format("%s is invalid ProgramType", programType));
      return false;
    }

    if (!store.programExists(Id.Program.from(namespace, appId, programId), type)) {
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           String.format("Program %s of Type %s in AppId %s in Namespace %s not present",
                                         programId, programType, appId, namespace));
      return false;
    }
    return true;
  }
}
