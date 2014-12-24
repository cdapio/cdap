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
import co.cask.http.HttpResponder;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
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
import javax.ws.rs.QueryParam;

/**
 * Dashboard HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces")
public class DashboardHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DashboardHttpHandler.class);
  private static final Gson GSON = new Gson();

  private final Map<String, String> inMemoryConfigStore;

  @Inject
  public DashboardHttpHandler(Authenticator authenticator) {
    super(authenticator);
    this.inMemoryConfigStore = Maps.newHashMap();
  }

  @Path("/{namespace-id}/configuration/dashboards")
  @POST
  public synchronized void createDashboard(final HttpRequest request, final HttpResponder responder,
                                           @PathParam("namespace-id") String namespace) throws Exception {
    String dashboardId = UUID.randomUUID().toString();
    inMemoryConfigStore.put(getDashboardId(namespace, dashboardId), GSON.toJson(decodeArguments(request)));
    responder.sendString(HttpResponseStatus.OK, dashboardId);
  }

  @Path("/{namespace-id}/configuration/dashboards")
  @GET
  public synchronized void listDashboard(final HttpRequest request, final HttpResponder responder,
                                         @PathParam("namespace-id") String namespace,
                                         @QueryParam("filter") String filter) throws Exception {
    List<String> dashboardList = Lists.newArrayList();
    for (String id : inMemoryConfigStore.keySet()) {
      List<String> parts = Lists.newArrayList(Splitter.on('.').omitEmptyStrings().split(id));
      if (parts.get(0).equals(namespace)) {
        dashboardList.add(parts.get(1));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, dashboardList);
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}")
  @DELETE
  public synchronized void deleteDashboard(final HttpRequest request, final HttpResponder responder,
                                           @PathParam("namespace-id") String namespace,
                                           @PathParam("dashboard-id") String id) throws Exception {
    String dashboardId = getDashboardId(namespace, id);
    if (!inMemoryConfigStore.containsKey(dashboardId)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      inMemoryConfigStore.remove(dashboardId);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties")
  @GET
  public synchronized void getProperties(final HttpRequest request, final HttpResponder responder,
                                         @PathParam("namespace-id") String namespace,
                                         @PathParam("dashboard-id") String id) throws Exception {
    String dashboardId = getDashboardId(namespace, id);
    if (!inMemoryConfigStore.containsKey(dashboardId)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      responder.sendString(HttpResponseStatus.OK, inMemoryConfigStore.get(dashboardId));
    }
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties")
  @PUT
  public synchronized void setProperties(final HttpRequest request, final HttpResponder responder,
                                         @PathParam("namespace-id") String namespace,
                                         @PathParam("dashboard-id") String id) throws Exception {
    String dashboardId = getDashboardId(namespace, id);
    if (!inMemoryConfigStore.containsKey(dashboardId)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      inMemoryConfigStore.put(dashboardId, GSON.toJson(decodeArguments(request)));
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  @Path("/{namespace-id}/configuration/dashboards/{dashboard-id}/properties")
  @DELETE
  public synchronized void deleteProperties(final HttpRequest request, final HttpResponder responder,
                                            @PathParam("namespace-id") String namespace,
                                            @PathParam("dashboard-id") String id) throws Exception {
    String dashboardId = getDashboardId(namespace, id);
    if (!inMemoryConfigStore.containsKey(dashboardId)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      inMemoryConfigStore.put(dashboardId, GSON.toJson(ImmutableMap.of()));
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  private String getDashboardId(String namespace, String id) {
    return String.format("%s.%s", namespace, id);
  }
}
