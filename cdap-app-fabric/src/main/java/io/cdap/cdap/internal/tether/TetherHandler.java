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
 */

package io.cdap.cdap.internal.tether;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link io.cdap.http.HttpHandler} to manage tehering v3 REST APIs that are common to client and server.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class TetherHandler extends AbstractHttpHandler {
  private static final Gson GSON = new GsonBuilder().create();

  private final CConfiguration cConf;
  private final TetherStore store;
  // Connection timeout in seconds.
  private int connectionTimeout;

  @Inject
  TetherHandler(CConfiguration cConf, TetherStore store) {
    this.cConf = cConf;
    this.store = store;
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    connectionTimeout = cConf.getInt(Constants.Tether.CONNECTION_TIMEOUT, Constants.Tether.CONNECTION_TIMEOUT_DEFAULT);
  }

  /**
   * Returns status of tethered peers.
   */
  @GET
  @Path("/tethering/connections")
  public void getTethers(HttpRequest request, HttpResponder responder) {
    List<PeerStatus> peerStatusList = new ArrayList<>();
    for (PeerInfo peer : store.getPeers()) {
      TetherConnectionStatus connectionStatus = TetherConnectionStatus.INACTIVE;
      if (System.currentTimeMillis() - peer.getLastConnectionTime() < connectionTimeout * 1000L) {
        connectionStatus = TetherConnectionStatus.ACTIVE;
      }
      peerStatusList.add(new PeerStatus(peer.getName(), peer.getEndpoint(), peer.getTetherStatus(),
                                        peer.getMetadata(), connectionStatus));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(peerStatusList));
  }

  /**
   * Returns tether status of a peer.
   */
  @GET
  @Path("/tethering/connections/{peer}")
  public void getTether(HttpRequest request, HttpResponder responder, @PathParam("peer") String peer) {
    PeerInfo peerInfo = store.getPeer(peer);
    TetherConnectionStatus connectionStatus = TetherConnectionStatus.INACTIVE;
    if (System.currentTimeMillis() - peerInfo.getLastConnectionTime() < connectionTimeout * 1000L) {
      connectionStatus = TetherConnectionStatus.ACTIVE;
    }
    PeerStatus peerStatus = new PeerStatus(peerInfo.getName(), peerInfo.getEndpoint(), peerInfo.getTetherStatus(),
                                           peerInfo.getMetadata(), connectionStatus);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(peerStatus));
  }

  /**
   * Deletes a tether.
   */
  @DELETE
  @Path("/tethering/connections/{peer}")
  public void deleteTether(HttpRequest request, HttpResponder responder, @PathParam("peer") String peer) {
    store.deletePeer(peer);
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
