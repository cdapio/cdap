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
import io.cdap.cdap.common.MethodNotAllowedException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.store.profile.ProfileStore;
import io.cdap.cdap.proto.provisioner.ProvisionerPropertyValue;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
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
 * {@link io.cdap.http.HttpHandler} to manage tethering v3 REST APIs that are common to client and server.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class TetherHandler extends AbstractHttpHandler {
  private static final Gson GSON = new GsonBuilder().create();
  // TODO: this should be defined by tethering provisioner
  static final String TETHER_PEER_NAME = "tetherPeer";

  private final CConfiguration cConf;
  private final TetherStore store;
  private TransactionRunner transactionRunner;
  // Connection timeout in seconds.
  private int connectionTimeout;

  @Inject
  TetherHandler(CConfiguration cConf, TetherStore store, TransactionRunner transactionRunner) {
    this.cConf = cConf;
    this.store = store;
    this.transactionRunner = transactionRunner;
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    connectionTimeout = cConf.getInt(Constants.Tether.CONNECTION_TIMEOUT_SECONDS,
                                     Constants.Tether.DEFAULT_CONNECTION_TIMEOUT_SECONDS);
  }

  /**
   * Returns status of tethered peers.
   */
  @GET
  @Path("/tethering/connections")
  public void getTethers(HttpRequest request, HttpResponder responder) {
    List<PeerStatus> peerStatusList = new ArrayList<>();
    for (PeerInfo peer : store.getPeers()) {
      peerStatusList.add(getPeerStatus(peer));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(peerStatusList));
  }

  /**
   * Returns tether status of a peer.
   */
  @GET
  @Path("/tethering/connections/{peer}")
  public void getTether(HttpRequest request, HttpResponder responder, @PathParam("peer") String peer) {
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(getPeerStatus(store.getPeer(peer))));
  }

  private PeerStatus getPeerStatus(PeerInfo peerInfo) {
    TetherConnectionStatus connectionStatus = TetherConnectionStatus.INACTIVE;
    if (System.currentTimeMillis() - peerInfo.getLastConnectionTime() < connectionTimeout * 1000L) {
      connectionStatus = TetherConnectionStatus.ACTIVE;
    }
    return new PeerStatus(peerInfo.getName(), peerInfo.getEndpoint(), peerInfo.getTetherStatus(),
                          peerInfo.getMetadata(), connectionStatus);
  }

  /**
   * Deletes a tether.
   */
  @DELETE
  @Path("/tethering/connections/{peer}")
  public void deleteTether(HttpRequest request, HttpResponder responder, @PathParam("peer") String peer) {
    // getPeer() throws NotFoundException if peer is not present
    store.getPeer(peer);
    if (cConf.getBoolean(Constants.Tether.TETHER_SERVER_ENABLE, Constants.Tether.DEFAULT_TETHER_SERVER_ENABLE)) {
      // Check if there are any compute profiles using this tether
      checkTetherUsedByProfile(peer);
    }
    store.deletePeer(peer);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private void checkTetherUsedByProfile(String peer) {
    TransactionRunners.run(transactionRunner, context -> {
      ProfileStore dataset = ProfileStore.get(context);
      List<String> profiles = dataset.findProfilesWithProperty(
        new ProvisionerPropertyValue(TETHER_PEER_NAME, peer, false));
      if (profiles.size() > 0) {
        throw new MethodNotAllowedException(String.format("Cannot delete tethering with peer %s" +
                                              " as it is used in compute profiles: %s", peer, profiles));
      }
    });
  }
}
