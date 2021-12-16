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
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ForbiddenException;
import io.cdap.cdap.common.NotImplementedException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * {@link io.cdap.http.HttpHandler} to manage tethering server v3 REST APIs
 */
@Path(Constants.Gateway.API_VERSION_3)
public class TetheringServerHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TetheringServerHandler.class);
  private static final Gson GSON = new GsonBuilder().create();
  private static final int MAX_CHUNK_SIZE = 4096;
  private final CConfiguration cConf;
  private final TetheringStore store;
  private final MessagingService messagingService;
  private final MultiThreadMessagingContext messagingContext;
  private final String topicPrefix;

  @Inject
  TetheringServerHandler(CConfiguration cConf, TetheringStore store, MessagingService messagingService) {
    this.cConf = cConf;
    this.store = store;
    this.messagingService = messagingService;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.topicPrefix = cConf.get(Constants.Tethering.TOPIC_PREFIX);
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
  }

  /**
   * Sends control commands to the client.
   */
  @GET
  @Path("/tethering/controlchannels/{peer}")
  public void connectControlChannel(FullHttpRequest request, HttpResponder responder, @PathParam("peer") String peer,
                                    @QueryParam("messageId") String messageId)
    throws IOException, NotImplementedException, PeerNotFoundException, ForbiddenException, BadRequestException {
    checkTetheringServerEnabled();
    store.updatePeerTimestamp(peer);
    TetheringStatus tetheringStatus = store.getPeer(peer).getTetheringStatus();
    if (tetheringStatus == TetheringStatus.PENDING) {
      throw new PeerNotFoundException(String.format("Peer %s not found", peer));
    } else if (tetheringStatus == TetheringStatus.REJECTED) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
      throw new ForbiddenException(String.format("Peer %s is not authorized", peer));
    }

    List<TetheringControlResponse> controlResponses = new ArrayList<>();
    MessageFetcher fetcher = messagingContext.getMessageFetcher();
    TopicId topic = new TopicId(NamespaceId.SYSTEM.getNamespace(), topicPrefix + peer);
    String lastMessageId = messageId;
    try (CloseableIterator<Message> iterator =
           fetcher.fetch(topic.getNamespace(), topic.getTopic(), 1, messageId)) {
      while (iterator.hasNext()) {
        Message message = iterator.next();
        TetheringControlMessage controlMessage = GSON.fromJson(message.getPayloadAsString(StandardCharsets.UTF_8),
                                                               TetheringControlMessage.class);
        lastMessageId = message.getId();
        controlResponses.add(new TetheringControlResponse(lastMessageId, controlMessage));
      }
    } catch (TopicNotFoundException e) {
      LOG.warn("Received control connection from peer {} that's not tethered", peer);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid message id %s", messageId));
    }

    if (controlResponses.isEmpty()) {
      controlResponses.add(new TetheringControlResponse(
        lastMessageId, new TetheringControlMessage(TetheringControlMessage.Type.KEEPALIVE)));
    }
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(controlResponses.toArray(new TetheringControlResponse[0]),
                                   TetheringControlResponse[].class));
  }

  /**
   * Creates a tethering with a client.
   */
  @PUT
  @Path("/tethering/connections/{peer}")
  public void createTethering(FullHttpRequest request, HttpResponder responder, @PathParam("peer") String peer)
    throws NotImplementedException, IOException {
    checkTetheringServerEnabled();

    String content = request.content().toString(StandardCharsets.UTF_8);
    TetheringConnectionRequest tetherRequest = GSON.fromJson(content, TetheringConnectionRequest.class);
    TopicId topicId = new TopicId(NamespaceId.SYSTEM.getNamespace(),
                                  topicPrefix + peer);
    try {
      messagingService.createTopic(new TopicMetadata(topicId, Collections.emptyMap()));
    } catch (TopicAlreadyExistsException e) {
      LOG.warn("Topic {} already exists", topicId);
    } catch (IOException e) {
      LOG.error("Failed to create topic {}", topicId, e);
      throw e;
    }

    // We don't need to keep track of the client metadata on the server side.
    PeerMetadata peerMetadata = new PeerMetadata(tetherRequest.getNamespaceAllocations(), Collections.emptyMap());
    // We don't store the peer endpoint on the server side because the connection is initiated by the client.
    PeerInfo peerInfo = new PeerInfo(peer, null, TetheringStatus.PENDING, peerMetadata);
    try {
      store.addPeer(peerInfo);
    } catch (PeerAlreadyExistsException pae) {
      // Peer is already configured, treat this as a no-op.
      responder.sendStatus(HttpResponseStatus.OK);
      return;
    } catch (Exception e) {
      try {
        messagingService.deleteTopic(topicId);
      } catch (Exception ex) {
        e.addSuppressed(ex);
      }
      throw new IOException("Failed to create tethering with peer " + peer, e);
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Accepts/rejects the tethering request.
   */
  @POST
  @Path("/tethering/connections/{peer}")
  public void tetheringAction(FullHttpRequest request, HttpResponder responder, @PathParam("peer") String peer)
    throws NotImplementedException, BadRequestException, PeerNotFoundException, IOException {
    String content = request.content().toString(StandardCharsets.UTF_8);
    TetheringActionRequest tetheringActionRequest = GSON.fromJson(content, TetheringActionRequest.class);
    TetheringStatus tetheringStatus;
    switch (tetheringActionRequest.getAction()) {
      case "accept":
        tetheringStatus = TetheringStatus.ACCEPTED;
        break;
      case "reject":
        tetheringStatus = TetheringStatus.REJECTED;
        break;
      default:
        throw new BadRequestException(String.format("Invalid action: %s", tetheringActionRequest.getAction()));
    }
    updateTetherStatus(responder, peer, tetheringStatus);
  }

  private void checkTetheringServerEnabled() throws NotImplementedException {
    if (!cConf.getBoolean(Constants.Tethering.TETHERING_SERVER_ENABLED)) {
      throw new NotImplementedException("Tethering is not enabled");
    }
  }

  private void updateTetherStatus(HttpResponder responder, String peer, TetheringStatus newStatus)
    throws NotImplementedException, PeerNotFoundException, IOException {
    checkTetheringServerEnabled();
    PeerInfo peerInfo = store.getPeer(peer);
    if (peerInfo.getTetheringStatus() == TetheringStatus.PENDING) {
      store.updatePeerStatus(peerInfo.getName(), newStatus);
    } else {
      LOG.info("Cannot update tether state to {} as current state state is {}",
               newStatus, peerInfo.getTetheringStatus());
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
