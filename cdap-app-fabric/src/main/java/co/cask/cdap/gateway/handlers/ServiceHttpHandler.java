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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 *  {@link HttpHandler} for User Services.
 */
@Path(Constants.Gateway.API_VERSION_2)
public class ServiceHttpHandler extends AbstractAppFabricHttpHandler {

  private final ProgramLifecycleHttpHandler programLifecycleHttpHandler;

  @Inject
  public ServiceHttpHandler(Authenticator authenticator,
                            ProgramLifecycleHttpHandler programLifecycleHttpHandler,
                            SecureHandler secureHandler) {
    super(authenticator, secureHandler);
    this.programLifecycleHttpHandler = programLifecycleHttpHandler;
  }

  /**
   * Returns a list of Services associated with an account.
   */
  @GET
  @Path("/services")
  public void getAllServices(HttpRequest request, HttpResponder responder) {
    programLifecycleHttpHandler.getAllServices(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Return the list of user Services in an application.
   */
  @Path("/apps/{app-id}/services")
  @GET
  public void getServicesByApp(HttpRequest request, HttpResponder responder, @PathParam("app-id") String appId) {
    programLifecycleHttpHandler.getProgramsByApp(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                                 ProgramType.SERVICE.getCategoryName());
  }

  /**
   * Return the number of instances for the given runnable of a service.
   */
  @GET
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-name}/instances")
  public void getInstances(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId,
                           @PathParam("service-id") String serviceId,
                           @PathParam("runnable-name") String runnableName) {
    programLifecycleHttpHandler.getServiceInstances(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                    appId, serviceId, runnableName);
  }

  /**
   * Set instances.
   */
  @PUT
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-name}/instances")
  public void setInstances(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId,
                           @PathParam("service-id") String serviceId,
                           @PathParam("runnable-name") String runnableName) {

    programLifecycleHttpHandler.setServiceInstances(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                    appId, serviceId, runnableName);
  }

  @GET
  @Path("/apps/{app-id}/services/{service-id}/live-info")
  public void serviceLiveInfo(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") String appId,
                              @PathParam("service-id") String serviceId) {
    programLifecycleHttpHandler.liveInfo(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                         ProgramType.SERVICE.getCategoryName(), serviceId);
  }
}
