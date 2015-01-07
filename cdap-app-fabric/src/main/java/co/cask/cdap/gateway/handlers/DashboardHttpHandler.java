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
import co.cask.cdap.config.ConfigStore;
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
import java.util.UUID;
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
  private static final String CONFIG_TYPE = "dashboard";
  private static final String PROPERTY_NAME = "config";
  private static final String ID = "id";

  private final ConfigStore configStore;

  @Inject
  public DashboardHttpHandler(Authenticator authenticator, ConfigStore configStore) {
    super(authenticator);
    this.configStore = configStore;
  }

  @Path("/")
  @POST
  public void create(final HttpRequest request, final HttpResponder responder,
                                  @PathParam("namespace-id") String namespace) throws Exception {
    String data = request.getContent().toString(Charsets.UTF_8);
    if (!data.equals("") && !isValidJSON(data)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
      return;
    }

    Map<String, String> propMap = ImmutableMap.of(PROPERTY_NAME, data);
    String dashboardId = UUID.randomUUID().toString();
    try {
      configStore.create(namespace, CONFIG_TYPE, new Config(dashboardId, propMap));
      Map<String, String> returnMap = ImmutableMap.of(ID, dashboardId);
      responder.sendJson(HttpResponseStatus.OK, returnMap);
    } catch (ConfigExistsException e) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Dashboard already exists");
    }
  }

  @Path("/")
  @GET
  public void list(final HttpRequest request, final HttpResponder responder,
                                @PathParam("namespace-id") String namespace) throws Exception {
    JsonArray jsonArray = new JsonArray();
    List<Config> configList = configStore.list(namespace, CONFIG_TYPE);
    for (Config config : configList) {
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty(ID, config.getId());
      jsonObject.add(PROPERTY_NAME, new JsonParser().parse(config.getProperties().get(PROPERTY_NAME)));
      jsonArray.add(jsonObject);
    }
    responder.sendJson(HttpResponseStatus.OK, jsonArray);
  }

  @Path("/{dashboard-id}")
  @DELETE
  public void delete(final HttpRequest request, final HttpResponder responder,
                                  @PathParam("namespace-id") String namespace,
                                  @PathParam("dashboard-id") String id) throws Exception {
    try {
      configStore.delete(namespace, CONFIG_TYPE, id);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (ConfigNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Dashboard not found");
    }
  }

  @Path("/{dashboard-id}")
  @GET
  public void get(final HttpRequest request, final HttpResponder responder,
                               @PathParam("namespace-id") String namespace,
                               @PathParam("dashboard-id") String id) throws Exception {
    try {
      Config dashConfig = configStore.get(namespace, CONFIG_TYPE, id);
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty(ID, id);
      jsonObject.add(PROPERTY_NAME, new JsonParser().parse(dashConfig.getProperties().get(PROPERTY_NAME)));
      responder.sendJson(HttpResponseStatus.OK, jsonObject);
    } catch (ConfigNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Dashboard not found");
    }
  }

  @Path("/{dashboard-id}")
  @PUT
  public void set(final HttpRequest request, final HttpResponder responder,
                               @PathParam("namespace-id") String namespace,
                               @PathParam("dashboard-id") String id) throws Exception {
    try {
      configStore.get(namespace, CONFIG_TYPE, id);
      String data = request.getContent().toString(Charsets.UTF_8);
      if (!isValidJSON(data)) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
        return;
      }

      Map<String, String> propMap = ImmutableMap.of(PROPERTY_NAME, data);
      configStore.update(namespace, CONFIG_TYPE, new Config(id, propMap));
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (ConfigNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Dashboard not found");
    }
  }

  private boolean isValidJSON(String json) {
    try {
      new JsonParser().parse(json);
    } catch (JsonSyntaxException ex) {
      return false;
    }
    return true;
  }
}
