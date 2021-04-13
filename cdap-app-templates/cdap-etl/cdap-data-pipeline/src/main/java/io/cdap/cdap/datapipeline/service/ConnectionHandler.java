/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.datapipeline.service;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.proto.id.NamespaceId;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class ConnectionHandler extends AbstractSystemHttpServiceHandler {
  private static final String API_VERSION = "v1";
  private static final Gson GSON = new Gson();

  private final Map<ConnectionId, Connection> connections = new ConcurrentHashMap<>();


  /**
   * Returns the list of connections in the given namespace
   */
  @GET
  @Path(API_VERSION + "/contexts/{context}/connections/")
  public void listConnections(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespace) {
    responder.sendJson(HttpURLConnection.HTTP_OK, GSON.toJson(connections.values()));
  }

  /**
   * Returns the specific connection information in the given namespace
   */
  @GET
  @Path(API_VERSION + "/contexts/{context}/connections/{connection}")
  public void getConnection(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace,
                            @PathParam("connection") String connection) {
    ConnectionId connectionId = new ConnectionId(new NamespaceId(namespace), connection);
    if (connections.containsKey(connectionId)) {
      responder.sendJson(HttpURLConnection.HTTP_OK, GSON.toJson(connections.get(connectionId)));
    } else {
      responder.sendStatus(HttpURLConnection.HTTP_NOT_FOUND);
    }
  }

  /**
   * Creates a connection in the given namespace
   */
  @PUT
  @Path(API_VERSION + "/contexts/{context}/connections/{connection}")
  public void createConnection(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("context") String namespace,
                               @PathParam("connection") String connection) {
    ConnectionCreationRequest creationRequest;
    try {
      creationRequest = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(),
                                      ConnectionCreationRequest.class);
    } catch (JsonSyntaxException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
      return;
    }

    Connection connectionInfo = new Connection(connection, creationRequest.getPlugin().getName(),
                                               creationRequest.getDescription(), false,
                                               System.currentTimeMillis(),
                                               creationRequest.getPlugin());
    connections.put(new ConnectionId(new NamespaceId(namespace), connection), connectionInfo);
    responder.sendStatus(HttpURLConnection.HTTP_OK);
  }

  /**
   * Delete a connection in the given namespace
   */
  @DELETE
  @Path(API_VERSION + "/contexts/{context}/connections/{connection}")
  public void deleteConnection(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("context") String namespace,
                               @PathParam("connection") String connection) {
    ConnectionId connectionId = new ConnectionId(new NamespaceId(namespace), connection);
    if (connections.containsKey(connectionId)) {
      connections.remove(connectionId);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    } else {
      responder.sendStatus(HttpURLConnection.HTTP_NOT_FOUND);
    }
  }
}
