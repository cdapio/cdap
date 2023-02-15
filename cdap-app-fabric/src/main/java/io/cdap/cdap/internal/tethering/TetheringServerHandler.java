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
import com.google.gson.JsonSyntaxException;
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
import io.cdap.cdap.internal.app.program.MessagingProgramStatePublisher;
import io.cdap.cdap.internal.app.program.ProgramStatePublisher;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.proto.security.InstancePermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link io.cdap.http.HttpHandler} to manage tethering server v3 REST APIs
 */
@Path(Constants.Gateway.API_VERSION_3)
public class TetheringServerHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TetheringServerHandler.class);
  private static final Gson GSON = new GsonBuilder().create();
  private final CConfiguration cConf;
  private final TetheringStore store;
  private final MessagingService messagingService;
  private final MultiThreadMessagingContext messagingContext;
  private final String topicPrefix;
  private final ContextAccessEnforcer contextAccessEnforcer;
  private final ProgramStatePublisher programStatePublisher;

  @Inject
  TetheringServerHandler(CConfiguration cConf, TetheringStore store, MessagingService messagingService,
                         ContextAccessEnforcer contextAccessEnforcer,
                         MessagingProgramStatePublisher programStatePublisher) {
    this.cConf = cConf;
    this.store = store;
    this.messagingService = messagingService;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.topicPrefix = cConf.get(Constants.Tethering.CLIENT_TOPIC_PREFIX);
    this.contextAccessEnforcer = contextAccessEnforcer;
    this.programStatePublisher = programStatePublisher;
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
  }

  /**
   * Sends control commands to the client and receives program status updates from the client.
   * NOTE: This endpoint is deprecated in favor of POST /tethering/channels/{peer}.
   */
  @POST
  @Path("/tethering/controlchannels/{peer}")
  public void connectControlChannel(FullHttpRequest request, HttpResponder responder, @PathParam("peer") String peer)
    throws IOException, NotImplementedException, PeerNotFoundException, ForbiddenException, BadRequestException {
    checkTetheringServerEnabled();
    store.updatePeerTimestamp(peer);
    TetheringStatus tetheringStatus = store.getPeer(peer).getTetheringStatus();
    String messageId = processRequestContent(request, peer);
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
    int batchSize = cConf.getInt(Constants.Tethering.CONTROL_MESSAGE_BATCH_SIZE);
    try (CloseableIterator<Message> iterator =
           fetcher.fetch(topic.getNamespace(), topic.getTopic(), batchSize, messageId)) {
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
   * Sends control commands to the client and receives program status updates from the client.
   */
  @POST
  @Path("/tethering/channels/{peer}")
  public void pollControlChannel(FullHttpRequest request, HttpResponder responder, @PathParam("peer") String peer)
    throws IOException, NotImplementedException, BadRequestException, TopicNotFoundException {
    checkTetheringServerEnabled();
    PeerInfo peerInfo;
    try {
      peerInfo = store.getPeer(peer);
    } catch (PeerNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           GSON.toJson(new TetheringControlResponseV2(Collections.emptyList(),
                                                                      TetheringStatus.NOT_FOUND)));
      return;
    }
    store.updatePeerTimestamp(peer);
    TetheringStatus tetheringStatus = peerInfo.getTetheringStatus();
    if (tetheringStatus != TetheringStatus.ACCEPTED) {
      // Don't send control messages to a peer that's not in ACCEPTED state.
      TetheringControlResponseV2 response = new TetheringControlResponseV2(Collections.emptyList(),
                                                                           tetheringStatus);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response));
      return;
    }

    String messageId = processRequestContent(request, peer);
    List<TetheringControlMessageWithId> controlMessages;
    try {
      controlMessages = getControlMessages(peer, messageId);
    } catch (TopicNotFoundException e) {
      // This can only happen if we added the peer during tethering creation, but crashed before creating the topic.
      // Recreate the topic and pull control messages again.
      TopicId topic = new TopicId(NamespaceId.SYSTEM.getNamespace(), topicPrefix + peer);
      createTopicIfNeeded(topic);
      controlMessages = getControlMessages(peer, messageId);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid message id %s", messageId));
    }

    TetheringControlResponseV2 response = new TetheringControlResponseV2(controlMessages,
                                                                         tetheringStatus);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response));
  }

  /**
   * Creates a tethering with a client.
   */
  @PUT
  @Path("/tethering/connections/{peer}")
  public void createTethering(FullHttpRequest request, HttpResponder responder, @PathParam("peer") String peer)
    throws NotImplementedException, IOException {
    checkTetheringServerEnabled();

    contextAccessEnforcer.enforce(InstanceId.SELF, InstancePermission.TETHER);

    String content = request.content().toString(StandardCharsets.UTF_8);
    TetheringConnectionRequest tetherRequest = GSON.fromJson(content, TetheringConnectionRequest.class);

    // We don't need to keep track of the client metadata on the server side.
    PeerMetadata peerMetadata = new PeerMetadata(tetherRequest.getNamespaceAllocations(), Collections.emptyMap(),
                                                 tetherRequest.getDescription());
    // We don't store the peer endpoint on the server side because the connection is initiated by the client.
    PeerInfo peerInfo = new PeerInfo(peer, null, TetheringStatus.PENDING, peerMetadata,
                                     tetherRequest.getRequestTime());
    if (store.writePeer(peerInfo)) {
      // Peer doesn't already exist. Create tethering topic.
      TopicId topicId = new TopicId(NamespaceId.SYSTEM.getNamespace(),
                                    topicPrefix + peer);
      createTopicIfNeeded(topicId);
    } else {
      // Recreate topic if the client deletes and recreates tethering.
      LOG.debug("Peer {} exists, recreating tethering topic", peer);
      deleteTopicForPeer(peer);
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Accepts/rejects the tethering request.
   */
  @POST
  @Path("/tethering/connections/{peer}")
  public void tetheringAction(FullHttpRequest request, HttpResponder responder, @PathParam("peer") String peer)
    throws NotImplementedException, BadRequestException, IOException {
    checkTetheringServerEnabled();

    String content = request.content().toString(StandardCharsets.UTF_8);
    TetheringActionRequest tetheringActionRequest = GSON.fromJson(content, TetheringActionRequest.class);
    PeerInfo peerInfo;
    try {
      peerInfo = store.getPeer(peer);
    } catch (PeerNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           GSON.toJson(new TetheringControlResponseV2(Collections.emptyList(),
                                                                      TetheringStatus.NOT_FOUND)));
      return;
    }
    if (peerInfo.getTetheringStatus() != TetheringStatus.PENDING) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }
    switch (tetheringActionRequest.getAction()) {
      case "accept":
        store.updatePeerStatus(peerInfo.getName(), TetheringStatus.ACCEPTED);
        responder.sendStatus(HttpResponseStatus.OK);
        break;
      case "reject":
        deleteTethering(peer);
        responder.sendStatus(HttpResponseStatus.OK);
        break;
      default:
        throw new BadRequestException(String.format("Invalid action: %s", tetheringActionRequest.getAction()));
    }
  }

  private List<TetheringControlMessageWithId> getControlMessages(String peer, @Nullable String afterMessageId)
    throws TopicNotFoundException, IOException {
    MessageFetcher fetcher = messagingContext.getMessageFetcher();
    TopicId topic = new TopicId(NamespaceId.SYSTEM.getNamespace(), topicPrefix + peer);
    int batchSize = cConf.getInt(Constants.Tethering.CONTROL_MESSAGE_BATCH_SIZE);
    List<TetheringControlMessageWithId> messages = new ArrayList<>();
    try (CloseableIterator<Message> iterator =
           fetcher.fetch(topic.getNamespace(), topic.getTopic(), batchSize, afterMessageId)) {
      while (iterator.hasNext()) {
        Message message = iterator.next();
        TetheringControlMessage controlMessage = GSON.fromJson(message.getPayloadAsString(StandardCharsets.UTF_8),
                                                               TetheringControlMessage.class);
        messages.add(new TetheringControlMessageWithId(controlMessage, message.getId()));
      }
    }
    return messages;
  }

  private void checkTetheringServerEnabled() throws NotImplementedException {
    if (!cConf.getBoolean(Constants.Tethering.TETHERING_SERVER_ENABLED)) {
      throw new NotImplementedException("Tethering is not enabled");
    }
  }

  private void deleteTethering(String peer) throws IOException {
    try {
      store.deletePeer(peer);
    } catch (PeerNotFoundException e) {
      // Peer doesn't exist, nothing to do here
    }
    deleteTopicForPeer(peer);
  }

  private void deleteTopicForPeer(String peer) throws IOException {
    TopicId topic = new TopicId(NamespaceId.SYSTEM.getNamespace(),
                                topicPrefix + peer);
    try {
      // If topic deletion fails here, the client will receive any leftover messages if it recreates tethering.
      // TODO(CDAP-19612): figure out how to handle this case better.
      messagingService.deleteTopic(topic);
    } catch (TopicNotFoundException e) {
      LOG.info("Topic {} was not found", topic.getTopic());
    }
  }

  /**
   * Processes and publishes the list of tethering program updates received from the client
   * Returns lastMessageId sent by the client
   */
  private String processRequestContent(FullHttpRequest request, String peer) throws BadRequestException {
    String lastControlMessageId;
    List<Notification> notificationList;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      TetheringControlChannelRequest content = GSON.fromJson(reader, TetheringControlChannelRequest.class);
      lastControlMessageId = content.getLastControlMessageId();
      notificationList = content.getNotificationList();
    } catch (JsonSyntaxException | IOException e) {
      throw new BadRequestException("Unable to parse request: " + e.getMessage(), e);
    }

    for (Notification notification : notificationList) {
      Map<String, String> properties = notification.getProperties();
      String programRunId = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
      String programStatus = properties.get(ProgramOptionConstants.PROGRAM_STATUS);

      LOG.debug("Received notification from peer {} about program run {} in state {}",
               peer, programRunId, programStatus);
      programStatePublisher.publish(notification.getNotificationType(), notification.getProperties());
    }
    return lastControlMessageId;
  }

  private void createTopicIfNeeded(TopicId topicId) throws IOException {
    try {
      messagingService.createTopic(new TopicMetadata(topicId, Collections.emptyMap()));
    } catch (TopicAlreadyExistsException ex) {
      // no-op
    }
  }
}
