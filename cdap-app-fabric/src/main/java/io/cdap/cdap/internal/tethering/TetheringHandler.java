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

package io.cdap.cdap.internal.tethering;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringConf;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringProvisioner;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerPropertyValue;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link io.cdap.http.HttpHandler} to manage tethering v3 REST APIs that are common to client and
 * server.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class TetheringHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TetheringHandler.class);
  private static final Gson GSON = new GsonBuilder().create();

  private final CConfiguration cConf;
  private final TetheringStore store;
  private final MessagingService messagingService;
  // Prefix of per-client TMS topic used on the tethering server.
  private final String clientTopicPrefix;
  // Prefix of program state TMS topic used on the tethering client.
  private final String programStateTopicPrefix;
  private final ProfileService profileService;

  // Connection timeout in seconds.
  private int connectionTimeout;

  @Inject
  TetheringHandler(CConfiguration cConf, TetheringStore store, MessagingService messagingService,
      ProfileService profileService) {
    this.cConf = cConf;
    this.store = store;
    this.messagingService = messagingService;
    this.clientTopicPrefix = cConf.get(Constants.Tethering.CLIENT_TOPIC_PREFIX);
    this.programStateTopicPrefix = cConf.get(Constants.Tethering.PROGRAM_STATE_TOPIC_PREFIX);
    this.profileService = profileService;
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    connectionTimeout = cConf.getInt(Constants.Tethering.CONNECTION_TIMEOUT_SECONDS,
        Constants.Tethering.DEFAULT_CONNECTION_TIMEOUT_SECONDS);
  }

  /**
   * Returns status of tethered peers.
   */
  @GET
  @Path("/tethering/connections")
  public void getTethers(HttpRequest request, HttpResponder responder) throws IOException {
    List<PeerState> peerStateList = new ArrayList<>();
    for (PeerInfo peer : store.getPeers()) {
      peerStateList.add(createPeerState(peer));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(peerStateList));
  }

  /**
   * Returns tether status of a peer.
   */
  @GET
  @Path("/tethering/connections/{peer}")
  public void getTether(HttpRequest request, HttpResponder responder,
      @PathParam("peer") String peer)
      throws PeerNotFoundException, IOException {
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(createPeerState(store.getPeer(peer))));
  }

  /**
   * Deletes a tether.
   */
  @DELETE
  @Path("/tethering/connections/{peer}")
  public void deleteTether(HttpRequest request, HttpResponder responder,
      @PathParam("peer") String peer)
      throws NotFoundException, IOException, BadRequestException {
    boolean tetheringServerEnabled = cConf.getBoolean(Constants.Tethering.TETHERING_SERVER_ENABLED);
    if (tetheringServerEnabled) {
      // Fail deletion if there are any profiles using this tethering
      List<Profile> tetheringProfiles = profileService.getProfiles(
          TetheringProvisioner.TETHERING_NAME,
          new ProvisionerPropertyValue(TetheringConf.TETHERED_INSTANCE_PROPERTY, peer, true));
      if (!tetheringProfiles.isEmpty()) {
        throw new BadRequestException(
            String.format("Cannot delete tethering as it's in use by compute profiles: %s."
                    + " Delete these profiles before deleting this tethering.",
                tetheringProfiles.stream().map(Profile::getName)
                    .collect(Collectors.toList())));
      }
    }

    // We need to delete subscriber state in addition to peer entry in tethering table on the client side.
    store.deletePeer(peer, !tetheringServerEnabled);
    // Remove per-tethering client topic if we're running on the server
    // If we're running on the client, remove per-tethering server program status update topic
    TopicId topic = tetheringServerEnabled ? new TopicId(NamespaceId.SYSTEM.getNamespace(),
        clientTopicPrefix + peer) :
        new TopicId(NamespaceId.SYSTEM.getNamespace(), programStateTopicPrefix + peer);
    try {
      messagingService.deleteTopic(topic);
    } catch (TopicNotFoundException e) {
      LOG.info("Topic {} was not found", topic.getTopic());
    }

    responder.sendStatus(HttpResponseStatus.OK);
  }

  private PeerState createPeerState(PeerInfo peerInfo) {
    TetheringConnectionStatus connectionStatus = TetheringConnectionStatus.INACTIVE;
    if (System.currentTimeMillis() - peerInfo.getLastConnectionTime() < connectionTimeout * 1000L) {
      connectionStatus = TetheringConnectionStatus.ACTIVE;
    }
    return new PeerState(peerInfo.getName(), peerInfo.getEndpoint(), peerInfo.getTetheringStatus(),
        peerInfo.getMetadata(), peerInfo.getRequestTime(), connectionStatus);
  }
}
