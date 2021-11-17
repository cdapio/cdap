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
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotImplementedException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link io.cdap.http.HttpHandler} to manage tethering server v3 REST APIs
 */
@Path(Constants.Gateway.API_VERSION_3)
public class TetherServerHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TetherServerHandler.class);
  private static final Gson GSON = new GsonBuilder().create();
  private static final String TETHERING_TOPIC_PREFIX = "tethering_";
  private static final String SUBSCRIBER = "tether.server";
  private final CConfiguration cConf;
  private final TetherStore store;
  private final MessagingService messagingService;
  private final MultiThreadMessagingContext messagingContext;
  private final TransactionRunner transactionRunner;
  // Last processed message id for each topic. There's a separate topic for each peer.
  private Map<String, String> lastMessageIds;

  @Inject
  TetherServerHandler(CConfiguration cConf, TetherStore store, MessagingService messagingService,
                      TransactionRunner transactionRunner) {
    this.cConf = cConf;
    this.store = store;
    this.messagingService = messagingService;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.transactionRunner = transactionRunner;
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    initializeMessageIds();
  }

  private void initializeMessageIds() {
    List<String> topics = store.getPeers().stream()
      .filter(p -> p.getTetherStatus() == TetherStatus.ACCEPTED)
      .map(PeerInfo::getName)
      .collect(Collectors.toList());
    lastMessageIds = new HashMap<>();
    for (String topic: topics) {
      TransactionRunners.run(transactionRunner, context -> {
        String messageId = AppMetadataStore.create(context).retrieveSubscriberState(topic, SUBSCRIBER);
        lastMessageIds.put(topic, messageId);
      });
    }
  }

  private void checkTetherServerEnabled() throws NotImplementedException {
    if (!cConf.getBoolean(Constants.Tether.TETHER_SERVER_ENABLE, Constants.Tether.DEFAULT_TETHER_SERVER_ENABLE)) {
      throw new NotImplementedException("Tethering is not enabled");
    }
  }

  /**
   * Sends control commands to the client.
   */
  @GET
  @Path("/tethering/controlchannels/{peer}")
  public void connectControlChannel(HttpRequest request, HttpResponder responder, @PathParam("peer") String peer)
    throws IOException, NotImplementedException {
    checkTetherServerEnabled();

    store.updatePeer(peer, System.currentTimeMillis());
    TetherStatus tetherStatus = store.getPeer(peer).getTetherStatus();
    if (tetherStatus == TetherStatus.PENDING) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    } else if (tetherStatus == TetherStatus.REJECTED) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
      return;
    }

    List<TetherControlMessage> commands = new ArrayList<>();
    MessageFetcher fetcher = messagingContext.getMessageFetcher();
    TopicId topic = new TopicId(NamespaceId.SYSTEM.getNamespace(), TETHERING_TOPIC_PREFIX + peer);
    String lastMessageId = null;
    try (CloseableIterator<Message> iterator =
           fetcher.fetch(topic.getNamespace(), topic.getTopic(), 1, lastMessageIds.get(topic.getTopic()))) {
      while (iterator.hasNext()) {
        Message message = iterator.next();
        TetherControlMessage controlMessage = GSON.fromJson(message.getPayloadAsString(StandardCharsets.UTF_8),
                                                            TetherControlMessage.class);
        commands.add(controlMessage);
        lastMessageId = message.getId();
      }
    } catch (TopicNotFoundException e) {
      LOG.warn("Received control connection from peer {} that's not tethered", peer);
    }

    if (lastMessageId != null) {
      // Update the last message id for the topic if we read any messages
      lastMessageIds.put(topic.getTopic(), lastMessageId);
      String finalLastMessageId = lastMessageId;
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore.create(context).persistSubscriberState(topic.getTopic(), SUBSCRIBER, finalLastMessageId);
      });
    }

    if (commands.isEmpty()) {
      commands.add(new TetherControlMessage(TetherControlMessage.Type.KEEPALIVE, null));
    }
    Type type = new TypeToken<List<TetherControlMessage>>() { }.getType();
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(commands, type));
  }

  /**
   * Creates a tether with a client.
   */
  @POST
  @Path("/tethering/connect")
  public void createTether(FullHttpRequest request, HttpResponder responder)
    throws NotImplementedException, IOException {
    checkTetherServerEnabled();

    String content = request.content().toString(StandardCharsets.UTF_8);
    TetherConnectionRequest tetherRequest = GSON.fromJson(content, TetherConnectionRequest.class);
    TopicId topicId = new TopicId(NamespaceId.SYSTEM.getNamespace(), "tethering_" + tetherRequest.getPeer());
    try {
      messagingService.createTopic(new TopicMetadata(topicId, Collections.emptyMap()));
    } catch (TopicAlreadyExistsException e) {
      LOG.warn("Topic {} already exists", topicId);
    } catch (IOException e) {
      LOG.error("Failed to create topic {}", topicId, e);
      throw e;
    }

    // We don't need to keep track of the client metadata on the server side.
    PeerMetadata peerMetadata = new PeerMetadata(tetherRequest.getNamespaces(), Collections.emptyMap());
    // We don't store the peer endpoint on the server side because the connection is initiated by the client.
    PeerInfo peerInfo = new PeerInfo(tetherRequest.getPeer(), null, TetherStatus.PENDING, peerMetadata);
    try {
      store.addPeer(peerInfo);
    } catch (Exception e) {
      try {
        messagingService.deleteTopic(topicId);
      } catch (Exception ex) {
        e.addSuppressed(ex);
      }
      throw new IOException("Failed to create tether with peer " + tetherRequest.getPeer(), e);
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Accepts the tethering request.
   */
  @POST
  @Path("/tethering/connections/{peer}/accept")
  public void acceptTether(HttpRequest request, HttpResponder responder, @PathParam("peer") String peer)
    throws NotImplementedException, BadRequestException {
    updateTetherStatus(responder, peer, TetherStatus.ACCEPTED);
  }

  /**
   * Rejects the tethering request.
   */
  @POST
  @Path("/tethering/connections/{peer}/reject")
  public void rejectTether(HttpRequest request, HttpResponder responder, @PathParam("peer") String peer)
    throws NotImplementedException, BadRequestException {
    updateTetherStatus(responder, peer, TetherStatus.REJECTED);
  }

  private void updateTetherStatus(HttpResponder responder, String peer, TetherStatus newStatus)
    throws NotImplementedException {
    checkTetherServerEnabled();
    PeerInfo peerInfo = store.getPeer(peer);
    if (peerInfo.getTetherStatus() == TetherStatus.PENDING) {
      store.updatePeer(peerInfo.getName(), newStatus);
    } else {
      LOG.info("Cannot update tether state to {} as current state state is {}",
               newStatus, peerInfo.getTetherStatus());
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Returns names of tethered peers.
   */
  @GET
  @Path("/tethering/connections/peers")
  public void getTethers(HttpRequest request, HttpResponder responder) {
    Type type = new TypeToken<List<String>>() { }.getType();
    List<String> peers = store.getPeers().stream().map(PeerInfo::getName).collect(Collectors.toList());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(peers, type));
  }

  /**
   * Returns namespaces exported by a peer.
   */
  @GET
  @Path("/tethering/connections/peers/{peer}/namespaces")
  public void getTethers(HttpRequest request, HttpResponder responder, @PathParam("peer") String peer) {
    Type type = new TypeToken<List<String>>() { }.getType();
    List<String> namespaces = store.getPeer(peer).getMetadata().getNamespaces().stream()
      .map(NamespaceAllocation::getNamespace).collect(Collectors.toList());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(namespaces, type));
  }

  // Currently unused.
  // TODO: Send RUN_PIPELINE message to peer.
  private void publishMessage(TopicId topicId, @Nullable TetherControlMessage message)
    throws IOException, TopicNotFoundException {
    MessagePublisher publisher = messagingContext.getMessagePublisher();
    try {
      publisher.publish(topicId.getNamespace(), topicId.getTopic(), StandardCharsets.UTF_8,
                        GSON.toJson(message));
    } catch (IOException | TopicNotFoundException e) {
      LOG.error("Failed to publish to topic {}", topicId, e);
      throw e;
    }
  }
}
