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
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
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
    String dashboardId = UUID.randomUUID().toString();
    String body = request.getContent().toString(Charsets.UTF_8);
    configStore.put(namespace, dashboardId, body);
    responder.sendString(HttpResponseStatus.OK, dashboardId);
  }

  @Path("/")
  @GET
  public synchronized void list(final HttpRequest request, final HttpResponder responder,
                                @PathParam("namespace-id") String namespace) throws Exception {
    Map<String, String> row = configStore.row(namespace);
    responder.sendJson(HttpResponseStatus.OK, row);
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
      responder.sendString(HttpResponseStatus.OK, configStore.get(namespace, id));
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
      String body = request.getContent().toString(Charsets.UTF_8);
      configStore.put(namespace, id, body);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }
}
