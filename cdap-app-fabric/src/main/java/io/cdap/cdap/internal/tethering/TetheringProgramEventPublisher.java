/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeProgramStatusSubscriberService;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.logging.gateway.handlers.ProgramRunRecordFetcher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads program status messages from TMS and writes to them to peer specific topics. {@link
 * TetheringAgentService} is responsible for reading status messages from the peer specific topics
 * and sending updates to the respective peers.
 */
public class TetheringProgramEventPublisher extends AbstractRetryableScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(TetheringProgramEventPublisher.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
          new GsonBuilder())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
      .create();
  static final String SUBSCRIBER = "tether.agent";

  private final TetheringStore store;
  private final MessagingService messagingService;
  private final MessageFetcher messageFetcher;
  private final MessagePublisher messagePublisher;
  private final ProgramRunRecordFetcher runRecordFetcher;
  private final TransactionRunner transactionRunner;
  private final String programUpdateTopic;
  private final int programUpdateFetchSize;
  private final String programStateTopicPrefix;
  private final long pollInterval;

  // Tracks id of last program status update message that was processed.
  private String lastProgramUpdateMessageId;

  @Inject
  TetheringProgramEventPublisher(CConfiguration cConf, TetheringStore store,
      MessagingService messagingService,
      ProgramRunRecordFetcher programRunRecordFetcher, TransactionRunner transactionRunner) {
    this(cConf, store, messagingService, new MultiThreadMessagingContext(messagingService).getMessageFetcher(),
         programRunRecordFetcher, transactionRunner);
  }

  @VisibleForTesting
  TetheringProgramEventPublisher(CConfiguration cConf, TetheringStore store,
                                 MessagingService messagingService,
                                 MessageFetcher messageFetcher,
                                 ProgramRunRecordFetcher programRunRecordFetcher, TransactionRunner transactionRunner) {
    super(RetryStrategies.fromConfiguration(cConf, "tethering.agent."));
    this.store = store;
    this.messagingService = messagingService;
    this.messageFetcher = messageFetcher;
    this.messagePublisher = new MultiThreadMessagingContext(messagingService).getMessagePublisher();
    this.runRecordFetcher = programRunRecordFetcher;
    this.transactionRunner = transactionRunner;
    this.programUpdateTopic = cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC);
    this.programUpdateFetchSize = cConf.getInt(Constants.AppFabric.STATUS_EVENT_FETCH_SIZE);
    this.programStateTopicPrefix = cConf.get(Constants.Tethering.PROGRAM_STATE_TOPIC_PREFIX);
    // TetheringAgentService reads messages published by TetheringProgramEventPublisher, so use the same
    // poll interval here
    this.pollInterval = TimeUnit.SECONDS.toMillis(
      cConf.getLong(Constants.Tethering.CONNECTION_INTERVAL));
  }

  @Override
  protected void doStartUp() {
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
      lastProgramUpdateMessageId = appMetadataStore.retrieveSubscriberState(programUpdateTopic,
          SUBSCRIBER);
      if (lastProgramUpdateMessageId == null) {
        // Initialize subscriber based on last message fetched by RuntimeProgramStatusSubscriberService.
        // This is to avoid fetching existing program history from before TetheringAgent was added
        String messageId = appMetadataStore.retrieveSubscriberState(programUpdateTopic,
            RuntimeProgramStatusSubscriberService.SUBSCRIBER);
        appMetadataStore.persistSubscriberState(programUpdateTopic, SUBSCRIBER, messageId);
        lastProgramUpdateMessageId = messageId;
      }
    });
  }

  @Override
  protected long runTask() throws IOException {
    List<PeerInfo> peers = store.getPeers().stream()
        // Ignore peers that are not in ACCEPTED state.
        .filter(p -> p.getTetheringStatus() == TetheringStatus.ACCEPTED)
        .collect(Collectors.toList());

    PeerProgramUpdates peerProgramUpdates = getPeerProgramUpdates();
    persistNotifications(peerProgramUpdates, peers);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
      appMetadataStore.persistSubscriberState(programUpdateTopic,
          SUBSCRIBER,
          lastProgramUpdateMessageId);

    });

    return pollInterval;
  }

  /**
   * Returns program status updates for tethered peers.
   */
  @VisibleForTesting
  PeerProgramUpdates getPeerProgramUpdates() {
    Map<String, List<Notification>> peerToNotifications = new HashMap<>();
    String lastMessageId = lastProgramUpdateMessageId;
    try (CloseableIterator<Message> iterator = messageFetcher.fetch(
        NamespaceId.SYSTEM.getNamespace(),
        programUpdateTopic,
        programUpdateFetchSize,
        lastProgramUpdateMessageId)) {
      while (iterator.hasNext()) {
        Message message = iterator.next();
        lastMessageId = message.getId();
        Notification notification = message.decodePayload(
            r -> GSON.fromJson(r, Notification.class));
        if (notification.getNotificationType() != Notification.Type.PROGRAM_STATUS) {
          continue;
        }
        Map<String, String> properties = notification.getProperties();
        String programRunId = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
        try {
          RunRecord runRecord = runRecordFetcher.getRunRecordMeta(
              GSON.fromJson(programRunId, ProgramRunId.class));
          if (runRecord.getPeerName() == null) {
            continue;
          }
          String programStatus = properties.get(ProgramOptionConstants.PROGRAM_STATUS);
          LOG.debug("Received message for peer {} about program run {} in state {}",
              runRecord.getPeerName(), programRunId, programStatus);
          peerToNotifications.computeIfAbsent(runRecord.getPeerName(), n -> new ArrayList<>())
              .add(notification);
        } catch (NotFoundException | IOException e) {
          LOG.error("Unable to fetch runRecord for programRunId {}", programRunId, e);
        }
      }
    } catch (Exception e) {
      LOG.error("Exception when fetching program updates. Will retry again during next poll", e);
    }
    return new PeerProgramUpdates(peerToNotifications, lastMessageId);
  }

  private void persistNotifications(PeerProgramUpdates peerProgramUpdates, List<PeerInfo> peers)
      throws IOException {
    for (PeerInfo peer : peers) {
      String peerName = peer.getName();
      List<String> notifications = peerProgramUpdates.peerToNotifications.getOrDefault(peerName,
              new ArrayList<>())
          .stream().map(n -> GSON.toJson(n)).collect(Collectors.toList());
      publishMessages(programStateTopicPrefix + peerName, notifications);
    }
    lastProgramUpdateMessageId = peerProgramUpdates.lastMessageId;
  }

  private void publishMessages(String topic, List<String> messages) throws IOException {
    if (messages.isEmpty()) {
      return;
    }
    Retries.runWithRetries(() -> {
      try {
        messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), topic,
            messages.toArray(new String[0]));
      } catch (TopicNotFoundException e) {
        // Create the topic if it doesn't exist and retry publish
        createTopicIfNeeded(new TopicId(NamespaceId.SYSTEM.getNamespace(), topic));
        throw new RetryableException(e);
      }
    }, RetryStrategies.limit(1, RetryStrategies.fixDelay(1, TimeUnit.SECONDS)));
  }

  private void createTopicIfNeeded(TopicId topicId) throws IOException {
    try {
      messagingService.createTopic(new TopicMetadata(topicId, Collections.emptyMap()));
      LOG.debug("Created topic {}", topicId.getTopic());
    } catch (TopicAlreadyExistsException ex) {
      // no-op
    }
  }

  /**
   * Program status notifications for tethered peers.
   */
  static class PeerProgramUpdates {

    // List of notifications for each tethered peer
    final Map<String, List<Notification>> peerToNotifications;
    // Last message id read from TMS
    final String lastMessageId;

    private PeerProgramUpdates(Map<String, List<Notification>> peerToNotifications,
        String lastMessageId) {
      this.peerToNotifications = peerToNotifications;
      this.lastMessageId = lastMessageId;
    }
  }

}
