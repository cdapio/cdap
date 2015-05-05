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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.config.Config;
import co.cask.cdap.config.ConfigExistsException;
import co.cask.cdap.config.ConfigNotFoundException;
import co.cask.cdap.config.DashboardStore;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
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
 * Dashboard HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/configuration/dashboards")
public class DashboardHttpHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DashboardHttpHandler.class);
  private static final JsonParser JSON_PARSER = new JsonParser();
  private static final String CONFIG_PROPERTY = "config";
  private static final String ID = "id";

  private final DashboardStore dashboardStore;

  @Inject
  public DashboardHttpHandler(Authenticator authenticator, DashboardStore dashboardStore) {
    super(authenticator);
    this.dashboardStore = dashboardStore;
  }

  @Path("/")
  @POST
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespace) throws Exception {
    String data = request.getContent().toString(Charsets.UTF_8);
    // Initialize with empty config if no data is sent during creation of the dashboard
    if (data.equals("")) {
      data = "{}";
    }

    if (!isValidJSON(data)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
      return;
    }

    //Dashboard Config has the following layout:
    //Config ID = dashboardId (randomUUID)
    //Config Properties = Map (Key = CONFIG_PROPERTY, value = Serialized JSON string of dashboard configuration)
    Map<String, String> propMap = ImmutableMap.of(CONFIG_PROPERTY, data);
    String dashboardId = "";
    try {
      dashboardId = dashboardStore.create(namespace, propMap);
      Map<String, String> returnMap = ImmutableMap.of(ID, dashboardId);
      responder.sendJson(HttpResponseStatus.OK, returnMap);
    } catch (ConfigExistsException e) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, String.format("Dashboard %s already exists", dashboardId));
    }
  }

  @Path("/")
  @GET
  public void list(HttpRequest request, HttpResponder responder,
                   @PathParam("namespace-id") String namespace) throws Exception {
    JsonArray jsonArray = new JsonArray();
    List<Config> configList = dashboardStore.list(namespace);
    for (Config config : configList) {
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty(ID, config.getId());
      jsonObject.add(CONFIG_PROPERTY, JSON_PARSER.parse(config.getProperties().get(CONFIG_PROPERTY)));
      jsonArray.add(jsonObject);
    }
    responder.sendJson(HttpResponseStatus.OK, jsonArray);
  }

  @Path("/{dashboard-id}")
  @DELETE
  public void delete(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespace,
                     @PathParam("dashboard-id") String id) throws Exception {
    try {
      dashboardStore.delete(namespace, id);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (ConfigNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Dashboard not found");
    }
  }

  @Path("/{dashboard-id}")
  @GET
  public void get(HttpRequest request, HttpResponder responder,
                  @PathParam("namespace-id") String namespace,
                  @PathParam("dashboard-id") String id) throws Exception {
    try {
      Config dashConfig = dashboardStore.get(namespace, id);
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty(ID, id);

      //Dashboard Config is stored in ConfigStore as serialized JSON string with CONFIG_PROPERTY key.
      //When we send the data back, we send it as JSON object instead of sending the serialized string.
      jsonObject.add(CONFIG_PROPERTY, JSON_PARSER.parse(dashConfig.getProperties().get(CONFIG_PROPERTY)));
      responder.sendJson(HttpResponseStatus.OK, jsonObject);
    } catch (ConfigNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Dashboard not found");
    }
  }

  @Path("/{dashboard-id}")
  @PUT
  public void set(HttpRequest request, HttpResponder responder,
                  @PathParam("namespace-id") String namespace,
                  @PathParam("dashboard-id") String id) throws Exception {
    try {
      String data = request.getContent().toString(Charsets.UTF_8);
      if (!isValidJSON(data)) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
        return;
      }

      Map<String, String> propMap = ImmutableMap.of(CONFIG_PROPERTY, data);
      dashboardStore.put(namespace, new Config(id, propMap));
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (ConfigNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Dashboard not found");
    }
  }

  private boolean isValidJSON(String json) {
    try {
      JSON_PARSER.parse(json);
    } catch (JsonSyntaxException ex) {
      return false;
    }
    return true;
  }
}
