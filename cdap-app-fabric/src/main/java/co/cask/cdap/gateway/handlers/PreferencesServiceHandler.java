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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.common.Scope;
import co.cask.cdap.app.config.ConfigService;
import co.cask.cdap.app.config.ConfigType;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.HttpResponder;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Configuration Service HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/configuration/preferences")
public class PreferencesServiceHandler extends ConfigServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PreferencesServiceHandler.class);
  private static final String INSTANCE_NAME = "cdap";

  private final Store store;

  @Inject
  public PreferencesServiceHandler(Authenticator authenticator,
                                   @Named(Constants.ConfigService.PREFERENCE_SETTING) ConfigService configService,
                                   StoreFactory storeFactory) {
    super(authenticator, configService);
    this.store = storeFactory.create();
  }

  //Instance Level properties
  @Path("/properties/{property-name}")
  @GET
  public void getInstanceProperty(final HttpRequest request, final HttpResponder responder,
                                  @PathParam("property-name") String property) throws Exception {
    getProperty(getPrefix(), ConfigType.PREFERENCES, INSTANCE_NAME, property, responder);
  }

  @Path("/properties/{property-name}")
  @PUT
  public void setInstanceProperty(final HttpRequest request, final HttpResponder responder,
                                  @PathParam("property-name") String property) throws Exception {
    setProperty(getPrefix(), ConfigType.PREFERENCES, INSTANCE_NAME, property, request, responder);
  }

  @Path("/properties")
  @POST
  public void setInstanceProperties(final HttpRequest request, final HttpResponder responder) throws Exception {
    setProperties(getPrefix(), ConfigType.PREFERENCES, INSTANCE_NAME, request, responder);
  }

  @Path("/properties")
  @GET
  public void getInstanceProperties(final HttpRequest request, final HttpResponder responder) throws Exception {
    getProperties(getPrefix(), ConfigType.PREFERENCES, INSTANCE_NAME, responder);
  }

  @Path("/properties/{property-name}")
  @DELETE
  public void deleteInstanceProperty(final HttpRequest request, final HttpResponder responder,
                                     @PathParam("property-name") String property) throws Exception {
    deleteProperty(getPrefix(), ConfigType.PREFERENCES, INSTANCE_NAME, property, responder);
  }

  @Path("/properties")
  @DELETE
  public void deleteInstanceProperties(final HttpRequest request, final HttpResponder responder) throws Exception {
    deleteProperties(getPrefix(), ConfigType.PREFERENCES, INSTANCE_NAME, responder);
  }

  //Namespace Level properties
  @Path("/namespaces/{namespace-id}/properties/{property-name}")
  @GET
  public void getNamespaceProperty(final HttpRequest request, final HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("property-name") String property) throws Exception {
    getProperty(getPrefix(INSTANCE_NAME), ConfigType.PREFERENCES, namespaceId, property, responder);
  }

  @Path("/namespaces/{namespace-id}/properties/{property-name}")
  @PUT
  public void putNamespaceProperty(final HttpRequest request, final HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("property-name") String property) throws Exception {
    //Check if the given Namespace is present
    if (store.getNamespace(Id.Namespace.from(namespaceId)) == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }
    setProperty(getPrefix(INSTANCE_NAME), ConfigType.PREFERENCES, namespaceId, property, request, responder);
  }

  @Path("/namespace/{namespace-id}/properties")
  @POST
  public void putNamespaceProperties(final HttpRequest request, final HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId) throws Exception {
    //Check if the given Namespace is present
    if (store.getNamespace(Id.Namespace.from(namespaceId)) == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }
    setProperties(getPrefix(INSTANCE_NAME), ConfigType.PREFERENCES, namespaceId, request, responder);
  }

  @Path("/namespace/{namespace-id}/properties")
  @GET
  public void getNamespaceProperties(final HttpRequest request, final HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId) throws Exception {
    getProperties(getPrefix(INSTANCE_NAME), ConfigType.PREFERENCES, namespaceId, responder);
  }

  @Path("/namespace/{namespace-id}/properties/{property-name}")
  @DELETE
  public void deleteNamespaceProperty(final HttpRequest request, final HttpResponder responder,
                                      @PathParam("namespace-id") String namespaceId,
                                      @PathParam("property-name") String property) throws Exception {
    deleteProperty(getPrefix(INSTANCE_NAME), ConfigType.PREFERENCES, namespaceId, property, responder);
  }

  @Path("/namespace/{namespace-id}/properties")
  @DELETE
  public void deleteNamespaceProperties(final HttpRequest request, final HttpResponder responder,
                                        @PathParam("namespace-id") String namespaceId) throws Exception {
    deleteProperties(getPrefix(INSTANCE_NAME), ConfigType.PREFERENCES, namespaceId, responder);
  }

  //Application Level properties
  @Path("/namespaces/{namespace-id}/apps/{app-id}/properties/{property-name}")
  @GET
  public void getApplicationProperty(final HttpRequest request, final HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-id") String appId,
                                     @PathParam("property-id") String propertyId) throws Exception {
    getProperty(getPrefix(INSTANCE_NAME, namespaceId), ConfigType.PREFERENCES, appId, propertyId, responder);
  }

  @Path("/namespaces/{namespace-id}/apps/{app-id}/properties/{property-name}")
  @PUT
  public void setApplicationProperty(final HttpRequest request, final HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-id") String appId,
                                     @PathParam("property-id") String propertyId) throws Exception {
    //Check if the application is present
    if (store.getApplication(Id.Application.from(namespaceId, appId)) == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }
    setProperty(getPrefix(INSTANCE_NAME, namespaceId), ConfigType.PREFERENCES, appId, propertyId, request, responder);
  }

  @Path("/namespace/{namespace-id}/apps/{app-id}/properties")
  @POST
  public void setApplicationProperties(final HttpRequest request, final HttpResponder responder,
                                       @PathParam("namespace-id") String namespaceId,
                                       @PathParam("app-id") String appId) throws Exception {
    //Check if the application is present
    if (store.getApplication(Id.Application.from(namespaceId, appId)) == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }
    setProperties(getPrefix(INSTANCE_NAME, namespaceId), ConfigType.PREFERENCES, appId, request, responder);
  }

  @Path("/namespace/{namespace-id}/apps/{app-id}/properties")
  @GET
  public void getApplicationProperties(final HttpRequest request, final HttpResponder responder,
                                       @PathParam("namespace-id") String namespaceId,
                                       @PathParam("app-id") String appId) throws Exception {
    getProperties(getPrefix(INSTANCE_NAME, namespaceId), ConfigType.PREFERENCES, appId, responder);
  }

  @Path("/namespace/{namespace-id}/apps/{app-id}/properties/{property-name}")
  @DELETE
  public void deleteApplicationProperty(final HttpRequest request, final HttpResponder responder,
                                        @PathParam("namespace-id") String namespaceId,
                                        @PathParam("app-id") String appId,
                                        @PathParam("property-name") String property) throws Exception {
    deleteProperty(getPrefix(INSTANCE_NAME, namespaceId), ConfigType.PREFERENCES, appId, property, responder);
  }

  @Path("/namespace/{namespace-id}/apps/{app-id}/properties")
  @DELETE
  public void deleteApplicationProperties(final HttpRequest request, final HttpResponder responder,
                                          @PathParam("namespace-id") String namespaceId,
                                          @PathParam("app-id") String appId) throws Exception {
    deleteProperties(getPrefix(INSTANCE_NAME, namespaceId), ConfigType.PREFERENCES, appId, responder);
  }

  //Program Level properties
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/properties/{property-name}")
  @GET
  public void getProgramProperty(final HttpRequest request, final HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("app-id") String appId,
                                 @PathParam("program-type") String programType,
                                 @PathParam("program-id") String programId,
                                 @PathParam("property-name") String property) throws Exception {
    try {
      getProperty(getPrefix(INSTANCE_NAME, namespaceId, appId), ConfigType.PREFERENCES,
                  getProgramName(programType, programId), property, responder);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid Program Type");
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/{property-name}")
  @PUT
  public void setProgramProperty(final HttpRequest request, final HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("app-id") String appId,
                                 @PathParam("program-type") String programType,
                                 @PathParam("program-id") String programId,
                                 @PathParam("property-name") String property) throws Exception {
    try {
      //Check if the program id/type is present.
      if (!store.programExists(Id.Program.from(namespaceId, appId, programId),
                               ProgramType.valueOfCategoryName(programType))) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        setProperty(getPrefix(INSTANCE_NAME, namespaceId, appId), ConfigType.PREFERENCES,
                    getProgramName(programType, programId), property, request, responder);
      }
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid Program Type");
    }
  }

  @Path("/namespace/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/properties")
  @POST
  public void setProgramProperties(final HttpRequest request, final HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("app-id") String appId,
                                   @PathParam("program-type") String programType,
                                   @PathParam("program-id") String programId) throws Exception {
    try {
      //Check if the program id/type is present.
      if (!store.programExists(Id.Program.from(namespaceId, appId, programId),
                               ProgramType.valueOfCategoryName(programType))) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        setProperties(getPrefix(INSTANCE_NAME, namespaceId, appId), ConfigType.PREFERENCES,
                      getProgramName(programType, programId), request, responder);
      }
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid Program Type");
    }
  }

  @Path("/namespace/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/properties")
  @GET
  public void getProgramProperties(final HttpRequest request, final HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("app-id") String appId,
                                   @PathParam("program-type") String programType,
                                   @PathParam("program-id") String programId) throws Exception {
    try {
      getProperties(getPrefix(INSTANCE_NAME, namespaceId, appId), ConfigType.PREFERENCES,
                    getProgramName(programType, programId), responder);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid Program Type");
    }
  }

  @Path("/namespace/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/properties/{property-name}")
  @DELETE
  public void deleteProgramProperty(final HttpRequest request, final HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-id") String appId,
                                    @PathParam("program-type") String programType,
                                    @PathParam("program-id") String programId,
                                    @PathParam("property-name") String property) throws Exception {
    try {
      deleteProperty(getPrefix(INSTANCE_NAME, namespaceId, appId), ConfigType.PREFERENCES,
                     getProgramName(programType, programId), property, responder);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid Program Type");
    }
  }

  @Path("/namespace/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/properties")
  @DELETE
  public void deleteProgramProperties(final HttpRequest request, final HttpResponder responder,
                                      @PathParam("namespace-id") String namespaceId,
                                      @PathParam("app-id") String appId,
                                      @PathParam("program-type") String programType,
                                      @PathParam("program-id") String programId) throws Exception {
    try {
      deleteProperties(getPrefix(INSTANCE_NAME, namespaceId, appId), ConfigType.PREFERENCES,
                       getProgramName(programType, programId), responder);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid Program Type");
    }
  }

  private String getProgramName(String programType, String programId) throws IllegalArgumentException {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    return String.format("%s.%s", type.getCategoryName(), programId);
  }

  private String getPrefix() {
    return getPrefix(null);
  }

  private String getPrefix(String instanceName) {
    return getPrefix(instanceName, null);
  }

  private String getPrefix(String instanceName, String namespace) {
    return getPrefix(instanceName, namespace, null);
  }

  private String getPrefix(String instanceName, String namespace, String appId) {
    List<String> list = Lists.newArrayList();
    list.add(Scope.INSTANCE.toString());
    if (instanceName != null) {
      list.add(INSTANCE_NAME);
      list.add(Scope.NAMESPACE.toString());
      if (namespace != null) {
        list.add(namespace);
        list.add(Scope.APPLICATION.toString());
        if (appId != null) {
          list.add(appId);
        }
      }
    }
    return Joiner.on(".").skipNulls().join(list);
  }
}
