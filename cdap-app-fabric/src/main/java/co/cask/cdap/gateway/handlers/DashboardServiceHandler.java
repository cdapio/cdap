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

/**
 * Dashboard Service HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/configuration/dashboard")
public class DashboardServiceHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DashboardServiceHandler.class);
  private static final Gson GSON = new Gson();
  private final ConfigService configService;

  @Inject
  public DashboardServiceHandler(Authenticator authenticator, ConfigService configService) {
    super(authenticator);
    this.configService = configService;
  }

  @Path("/")
  @POST
  public void createDashboard(final HttpRequest request, final HttpResponder responder) throws Exception {
    String id = configService.createConfig(ConfigType.DASHBOARD, getAuthenticatedAccountId(request));
    Map<String, String> settings = decodeArguments(request);
    if (settings != null) {
      configService.writeSetting(ConfigType.DASHBOARD, id, settings);
    }
    responder.sendString(HttpResponseStatus.OK, id);
  }

  @Path("/")
  @GET
  public void listDashboard(final HttpRequest request, final HttpResponder responder) throws Exception {
    //TODO: Check for filter=all and return all dashboards for which user has read permission
    List<String> dashboardIds = configService.getConfig(ConfigType.DASHBOARD, getAuthenticatedAccountId(request));
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(dashboardIds));
  }

  @Path("/{dashboard-id}")
  @DELETE
  public void deleteDashboard(final HttpRequest request, final HttpResponder responder,
                              @PathParam("dashboard-id") String dashboard) throws Exception {
    //TODO: The accId should be the owner of the dashboard!
    if (!isDashboardPresent(dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      configService.deleteConfig(ConfigType.DASHBOARD, getAuthenticatedAccountId(request), dashboard);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  @Path("/{dashboard-id}/properties")
  @GET
  public void getProperties(final HttpRequest request, final HttpResponder responder,
                          @PathParam("dashboard-id") String dashboard) throws Exception {
    if (!isDashboardPresent(dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      Map<String, String> settings = configService.readSetting(ConfigType.DASHBOARD, dashboard);
      responder.sendString(HttpResponseStatus.OK, GSON.toJson(settings));
    }
  }

  @Path("/{dashboard-id}/properties")
  @POST
  public void setProperties(final HttpRequest request, final HttpResponder responder,
                            @PathParam("dashboard-id") String dashboard) throws Exception {
    if (!isDashboardPresent(dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      configService.writeSetting(ConfigType.DASHBOARD, dashboard, decodeArguments(request));
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  @Path("/{dashboard-id}/properties")
  @DELETE
  public void deleteProperties(final HttpRequest request, final HttpResponder responder,
                               @PathParam("dashboard-id") String dashboard) throws Exception {
    //TODO: Implement!
  }

  @Path("/{dashboard-id}/properties/{property-name}")
  @GET
  public void getProperty(final HttpRequest request, final HttpResponder responder,
                          @PathParam("dashboard-id") String dashboard, @PathParam("property-name") String property)
    throws Exception {
    if (!isDashboardPresent(dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      String value = configService.readSetting(ConfigType.DASHBOARD, dashboard, property);
      responder.sendString(HttpResponseStatus.OK, value);
    }
  }

  @Path("/{dashboard-id}/properties/{property-name}")
  @PUT
  public void putProperty(final HttpRequest request, final HttpResponder responder,
                          @PathParam("dashboard-id") String dashboard, @PathParam("property-name") String property)
    throws Exception {
    if (!isDashboardPresent(dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      String value = parseBody(request, String.class);
      configService.writeSetting(ConfigType.DASHBOARD, dashboard, property, value);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  @Path("/{dashboard-id}/properties/{property-name}")
  @DELETE
  public void deleteProperty(final HttpRequest request, final HttpResponder responder,
                             @PathParam("dashboard-id") String dashboard, @PathParam("property-name") String property)
    throws Exception {
    if (!isDashboardPresent(dashboard)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      configService.deleteSetting(ConfigType.DASHBOARD, dashboard, property);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  private boolean isDashboardPresent(String id) throws Exception {
    return configService.checkConfig(ConfigType.DASHBOARD, id);
  }
}
