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
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  //TODO: https://issues.cask.co/browse/CDAP-699 PersistenceStore will be used instead of inMemory implementation.
  private final Table<String, String, String> configStore;

  @Inject
  public DashboardHttpHandler(Authenticator authenticator) {
    super(authenticator);
    this.configStore = HashBasedTable.create();
  }

  @Path("/")
  @POST
  public synchronized void create(final HttpRequest request, final HttpResponder responder,
                                  @PathParam("namespace-id") String namespace) throws Exception {
    String data = request.getContent().toString(Charsets.UTF_8);
    if (!data.equals("") && !isValidJSON(data)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
      return;
    }

    Map<String, String> jsonMap = Maps.newHashMap();
    String dashboardId = UUID.randomUUID().toString();
    configStore.put(namespace, dashboardId, data);
    jsonMap.put("id", dashboardId);
    responder.sendJson(HttpResponseStatus.OK, jsonMap);
  }

  @Path("/")
  @GET
  public synchronized void list(final HttpRequest request, final HttpResponder responder,
                                @PathParam("namespace-id") String namespace) throws Exception {
    JsonArray jsonArray = new JsonArray();
    Map<String, String> row = configStore.row(namespace);
    for (Map.Entry<String, String> dash : row.entrySet()) {
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("id", dash.getKey());
      jsonObject.add("config", new JsonParser().parse(dash.getValue()));
      jsonArray.add(jsonObject);
    }
    responder.sendJson(HttpResponseStatus.OK, jsonArray);
  }

  @Path("/{dashboard-id}")
  @DELETE
  public synchronized void delete(final HttpRequest request, final HttpResponder responder,
                                  @PathParam("namespace-id") String namespace,
                                  @PathParam("dashboard-id") String id) throws Exception {
    if (!configStore.contains(namespace, id)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      configStore.remove(namespace, id);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  @Path("/{dashboard-id}")
  @GET
  public synchronized void get(final HttpRequest request, final HttpResponder responder,
                               @PathParam("namespace-id") String namespace,
                               @PathParam("dashboard-id") String id) throws Exception {
    if (!configStore.contains(namespace, id)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("id", id);
      jsonObject.add("config", new JsonParser().parse(configStore.get(namespace, id)));
      responder.sendJson(HttpResponseStatus.OK, jsonObject);
    }
  }

  @Path("/{dashboard-id}")
  @PUT
  public synchronized void set(final HttpRequest request, final HttpResponder responder,
                               @PathParam("namespace-id") String namespace,
                               @PathParam("dashboard-id") String id) throws Exception {
    if (!configStore.contains(namespace, id)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      String data = request.getContent().toString(Charsets.UTF_8);
      if (!isValidJSON(data)) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
        return;
      }

      configStore.put(namespace, id, data);
      responder.sendStatus(HttpResponseStatus.OK);
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
