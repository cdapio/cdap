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
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.inject.Inject;
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
public class DashboardServiceHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DashboardServiceHandler.class);
  private static final Gson GSON = new Gson();
  private final ConfigService configService;

  @Inject
  public DashboardServiceHandler(Authenticator authenticator, ConfigService configService) {
    super(authenticator);
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
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(dashboardIds));
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}")
  @DELETE
  public void deleteDashboard(final HttpRequest request, final HttpResponder responder,
                              @PathParam("namespace-id") String namespace,
                              @PathParam("dashboard-id") String dashboard) throws Exception {
    //TODO: Only the owner of the dashboard can delete the dashboard?
    if (!isDashboardPresent(namespace, dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      configService.deleteConfig(namespace, ConfigType.DASHBOARD, getAuthenticatedAccountId(request), dashboard);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties")
  @GET
  public void getProperties(final HttpRequest request, final HttpResponder responder,
                            @PathParam("namespace-id") String namespace,
                            @PathParam("dashboard-id") String dashboard) throws Exception {
    if (!isDashboardPresent(namespace, dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      Map<String, String> settings = configService.readSetting(namespace, ConfigType.DASHBOARD, dashboard);
      responder.sendString(HttpResponseStatus.OK, GSON.toJson(settings));
    }
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties")
  @POST
  public void setProperties(final HttpRequest request, final HttpResponder responder,
                            @PathParam("namespace-id") String namespace,
                            @PathParam("dashboard-id") String dashboard) throws Exception {
    if (!isDashboardPresent(namespace, dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      configService.writeSetting(namespace, ConfigType.DASHBOARD, dashboard, decodeArguments(request));
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties")
  @DELETE
  public void deleteProperties(final HttpRequest request, final HttpResponder responder,
                               @PathParam("namespace-id") String namespace,
                               @PathParam("dashboard-id") String dashboard) throws Exception {
    if (!isDashboardPresent(namespace, dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      configService.deleteSetting(namespace, ConfigType.DASHBOARD, dashboard);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties/{property-name}")
  @GET
  public void getProperty(final HttpRequest request, final HttpResponder responder,
                          @PathParam("namespace-id") String namespace,
                          @PathParam("dashboard-id") String dashboard, @PathParam("property-name") String property)
    throws Exception {
    if (!isDashboardPresent(namespace, dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      String value = configService.readSetting(namespace, ConfigType.DASHBOARD, dashboard, property);
      responder.sendString(HttpResponseStatus.OK, value);
    }
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties/{property-name}")
  @PUT
  public void putProperty(final HttpRequest request, final HttpResponder responder,
                          @PathParam("namespace-id") String namespace,
                          @PathParam("dashboard-id") String dashboard, @PathParam("property-name") String property)
    throws Exception {
    if (!isDashboardPresent(namespace, dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      String value = parseBody(request, String.class);
      configService.writeSetting(namespace, ConfigType.DASHBOARD, dashboard, property, value);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties/{property-name}")
  @DELETE
  public void deleteProperty(final HttpRequest request, final HttpResponder responder,
                             @PathParam("namespace-id") String namespace,
                             @PathParam("dashboard-id") String dashboard, @PathParam("property-name") String property)
    throws Exception {
    if (!isDashboardPresent(namespace, dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      configService.deleteSetting(namespace, ConfigType.DASHBOARD, dashboard, property);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  private boolean isDashboardPresent(String namespace, String id) throws Exception {
    return configService.checkConfig(namespace, ConfigType.DASHBOARD, id);
  }
}
