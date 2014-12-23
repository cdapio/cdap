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

import co.cask.cdap.app.config.ConfigService;
import co.cask.cdap.app.config.ConfigType;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Dashboard Service HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class DashboardServiceHandler extends ConfigServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DashboardServiceHandler.class);

  private final ConfigService configService;

  @Inject
  public DashboardServiceHandler(Authenticator authenticator,
                                 @Named(Constants.ConfigService.DASHBOARD) ConfigService configService) {
    super(authenticator, configService);
    this.configService = configService;
  }

  @Path("/{namespace-id}/configuration/dashboards")
  @POST
  public void createDashboard(final HttpRequest request, final HttpResponder responder,
                              @PathParam("namespace-id") String namespace) throws Exception {
    String id = configService.createConfig(namespace, ConfigType.DASHBOARD, getAuthenticatedAccountId(request));
    Map<String, String> settings = decodeArguments(request);
    if (settings != null) {
      configService.writeSetting(namespace, ConfigType.DASHBOARD, id, settings);
    }
    responder.sendString(HttpResponseStatus.OK, id);
  }

  @Path("/{namespace-id}/configuration/dashboards")
  @GET
  public void listDashboard(final HttpRequest request, final HttpResponder responder,
                            @PathParam("namespace-id") String namespace,
                            @QueryParam("filter") String filter) throws Exception {
    List<String> dashboardIds;
    if (filter != null && filter.equals("all")) {
      dashboardIds = configService.getConfig(namespace, ConfigType.DASHBOARD);
    } else {
      dashboardIds = configService.getConfig(namespace, ConfigType.DASHBOARD, getAuthenticatedAccountId(request));
    }
    responder.sendJson(HttpResponseStatus.OK, dashboardIds);
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}")
  @DELETE
  public void deleteDashboard(final HttpRequest request, final HttpResponder responder,
                              @PathParam("namespace-id") String namespace,
                              @PathParam("dashboard-id") String dashboard) throws Exception {
    //TODO: Only the owner of the dashboard can delete the dashboard?
    deleteConfig(namespace, ConfigType.DASHBOARD, dashboard, request, responder);
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties")
  @GET
  public void getProperties(final HttpRequest request, final HttpResponder responder,
                            @PathParam("namespace-id") String namespace,
                            @PathParam("dashboard-id") String dashboard) throws Exception {
    getProperties(namespace, ConfigType.DASHBOARD, dashboard, responder);
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties")
  @POST
  public void setProperties(final HttpRequest request, final HttpResponder responder,
                            @PathParam("namespace-id") String namespace,
                            @PathParam("dashboard-id") String dashboard) throws Exception {
    setProperties(namespace, ConfigType.DASHBOARD, dashboard, request, responder);
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties")
  @DELETE
  public void deleteProperties(final HttpRequest request, final HttpResponder responder,
                               @PathParam("namespace-id") String namespace,
                               @PathParam("dashboard-id") String dashboard) throws Exception {
    deleteProperties(namespace, ConfigType.DASHBOARD, dashboard, responder);
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties/{property-name}")
  @GET
  public void getProperty(final HttpRequest request, final HttpResponder responder,
                          @PathParam("namespace-id") String namespace,
                          @PathParam("dashboard-id") String dashboard, @PathParam("property-name") String property)
    throws Exception {
    getProperty(namespace, ConfigType.DASHBOARD, dashboard, property, responder);
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties/{property-name}")
  @PUT
  public void putProperty(final HttpRequest request, final HttpResponder responder,
                          @PathParam("namespace-id") String namespace,
                          @PathParam("dashboard-id") String dashboard, @PathParam("property-name") String property)
    throws Exception {
    setProperty(namespace, ConfigType.DASHBOARD, dashboard, property, request, responder);
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties/{property-name}")
  @DELETE
  public void deleteProperty(final HttpRequest request, final HttpResponder responder,
                             @PathParam("namespace-id") String namespace,
                             @PathParam("dashboard-id") String dashboard, @PathParam("property-name") String property)
    throws Exception {
    deleteProperty(namespace, ConfigType.DASHBOARD, dashboard, property, responder);
  }
}
