/*
 * Copyright © 2021-2022 Cask Data, Inc.
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
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedProgramRunner;
import io.cdap.cdap.internal.provision.ProvisionerNotifier;
import io.cdap.cdap.internal.tethering.proto.v1.TetheringLaunchMessage;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import javax.annotation.Nullable;
import javax.inject.Named;
import org.apache.commons.io.IOUtils;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main class to run the remote agent service.
 */
public class TetheringAgentService extends AbstractRetryableScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(TetheringAgentService.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
          new GsonBuilder())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
      .create();
  public static final String REMOTE_TETHERING_AUTHENTICATOR = "remoteTetheringAuthenticator";

  private final CConfiguration cConf;
  private final long connectionInterval;
  private final TetheringStore store;
  private final ProgramStateWriter programStateWriter;
  private final MessageFetcher messageFetcher;
  private final LocationFactory locationFactory;
  private final ProvisionerNotifier provisionerNotifier;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final int programUpdateFetchSize;
  private final TetheringClient tetheringClient;
  private final String programStateTopicPrefix;
  private SubscriberState subscriberState;

  @Inject
  TetheringAgentService(CConfiguration cConf, TetheringStore store,
      ProgramStateWriter programStateWriter, MessagingService messagingService,
      @Named(REMOTE_TETHERING_AUTHENTICATOR) RemoteAuthenticator remoteAuthenticator,
      LocationFactory locationFactory,
      ProvisionerNotifier provisionerNotifier,
      NamespaceQueryAdmin namespaceQueryAdmin) {
    super(RetryStrategies.fromConfiguration(cConf, "tethering.agent."));
    this.connectionInterval = TimeUnit.SECONDS.toMillis(
        cConf.getLong(Constants.Tethering.CONNECTION_INTERVAL));
    this.cConf = cConf;
    this.programStateTopicPrefix = cConf.get(Constants.Tethering.PROGRAM_STATE_TOPIC_PREFIX);
    this.store = store;
    this.programStateWriter = programStateWriter;
    this.messageFetcher = new MultiThreadMessagingContext(messagingService).getMessageFetcher();
    this.locationFactory = locationFactory;
    this.provisionerNotifier = provisionerNotifier;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.tetheringClient = new TetheringClient(remoteAuthenticator, cConf);
    this.programUpdateFetchSize = cConf.getInt(Constants.AppFabric.STATUS_EVENT_FETCH_SIZE);
    this.subscriberState = new SubscriberState();
  }

  @Override
  protected void doStartUp() {
    initializeMessageIds();
  }

  /**
   * Creates a map between peered instances and the list of Notifications to be sent over. Then
   * connects to the control channel and processes the TetheringControlResponses.
   */
  @Override
  protected long runTask() throws IOException {
    List<PeerInfo> peers = store.getPeers().stream()
        // Ignore peers in REJECTED state.
        .filter(p -> p.getTetheringStatus() != TetheringStatus.REJECTED)
        .collect(Collectors.toList());

    for (PeerInfo peer : peers) {
      // Endpoint should never be null here. Endpoint is only null on the server side.
      Preconditions.checkArgument(peer.getEndpoint() != null,
          "Peer %s doesn't have an endpoint", peer.getName());
      String lastControlMessageId = subscriberState.getLastMessageIdReceived(peer.getName());
      ProgramStateUpdates p = getProgramStateUpdates(peer.getName());
      TetheringControlChannelRequest channelRequest = new TetheringControlChannelRequest(
          lastControlMessageId,
          p.notifications);
      TetheringControlResponseV2 response;
      try {
        response = tetheringClient.pollControlChannel(peer, channelRequest);
      } catch (Exception e) {
        LOG.debug("Failed to create control channel to {}", peer.getName(), e);
        continue;
      }
      subscriberState.setLastMessageIdSent(peer.getName(), p.lastMessageId);

      switch (response.getTetheringStatus()) {
        case PENDING:
          store.updatePeerTimestamp(peer.getName());
          break;
        case ACCEPTED:
          store.updatePeerStatusAndTimestamp(peer.getName(), TetheringStatus.ACCEPTED);
          String lastMessageId = processTetheringControlResponse(response, peer);
          if (lastMessageId != null) {
            subscriberState.setLastMessageIdReceived(peer.getName(), lastMessageId);
          }
          break;
        case REJECTED:
        case NOT_FOUND:
          store.updatePeerStatusAndTimestamp(peer.getName(), TetheringStatus.REJECTED);
          break;
        default:
          LOG.error("Received unexpected tethering status {} from peer {}",
              response.getTetheringStatus(), peer.getName());
      }
    }

    // Update last message ids in the store for all peers
    store.updateSubscriberState(subscriberState);

    return connectionInterval;
  }

  private void initializeMessageIds() {
    List<String> peers;
    try {
      peers = store.getPeers().stream()
          .map(PeerInfo::getName)
          .collect(Collectors.toList());
    } catch (IOException e) {
      LOG.warn("Failed to get peer information", e);
      return;
    }

    subscriberState = store.initializeSubscriberState(peers);
  }

  /**
   * Processes control messages sent by the server and returns the id of the last processed message.
   * Returns null if no messages were processed.
   */
  @Nullable
  private String processTetheringControlResponse(TetheringControlResponseV2 response,
      PeerInfo peerInfo) {
    String lastMessageId = null;
    for (TetheringControlMessageWithId messageWithId : response.getControlMessages()) {
      TetheringControlMessage controlMessage = messageWithId.getControlMessage();
      lastMessageId = messageWithId.getMessageId();
      try {
        processControlMessage(controlMessage, peerInfo);
      } catch (IOException e) {
        LOG.warn("Failed to process control message of type {} from peer {}",
            controlMessage.getType(), peerInfo.getName(), e);
        return lastMessageId;
      }
    }
    return lastMessageId;
  }

  @VisibleForTesting
  void processControlMessage(TetheringControlMessage controlMessage, PeerInfo peerInfo)
      throws IOException {
    LOG.trace("Got message type {} from {}", controlMessage.getType(), peerInfo.getName());
    switch (controlMessage.getType()) {
      case KEEPALIVE:
        break;
      case START_PROGRAM:
        publishStartProgram(Bytes.toString(controlMessage.getPayload()), peerInfo);
        break;
      case STOP_PROGRAM:
        publishStopProgram(Bytes.toString(controlMessage.getPayload()));
        break;
      case KILL_PROGRAM:
        publishKillProgram(Bytes.toString(controlMessage.getPayload()));
        break;
    }
  }

  /**
   * Modify received files to reference the namespace in this instance and to include the peer name
   * for RunRecord. Use these files to start the program through ProgramStateWriter. Save the rest
   * of the files into a temp dir (to be loaded when the Program is created in
   * DistributedProgramRunner).
   */
  private void publishStartProgram(String runPayload, PeerInfo peerInfo) throws IOException {
    TetheringLaunchMessage message = GSON.fromJson(runPayload, TetheringLaunchMessage.class);

    CConfiguration cConfCopy = CConfiguration.copy(cConf);
    for (Map.Entry<String, String> cConfEntry : message.getCConfEntries().entrySet()) {
      cConfCopy.set(cConfEntry.getKey(), cConfEntry.getValue());
    }
    // Don't upload event logs for tethered runs as it breaks pipelines that read from HDFS.
    // See https://cdap.atlassian.net/browse/CDAP-20959?focusedCommentId=53167 for details.
    cConfCopy.setBoolean(Constants.AppFabric.SPARK_EVENT_LOGS_ENABLED, false);

    Map<String, String> files = new HashMap<>();
    for (Map.Entry<String, byte[]> file : message.getFiles().entrySet()) {
      try (GZIPInputStream inputStream = new GZIPInputStream(
          new ByteArrayInputStream(file.getValue()))) {
        files.put(file.getKey(), IOUtils.toString(inputStream));
      }
    }

    ApplicationSpecification appSpec = GSON.fromJson(
        files.get(DistributedProgramRunner.APP_SPEC_FILE_NAME),
        ApplicationSpecification.class);
    ProgramOptions programOpts = GSON.fromJson(
        files.get(DistributedProgramRunner.PROGRAM_OPTIONS_FILE_NAME),
        ProgramOptions.class);
    ProgramId programId = programOpts.getProgramId();
    ProgramRunId programRunId = programId.run(
        programOpts.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, appSpec);

    Location programDir = locationFactory.create(
        String.format("%s/%s", cConf.get(Constants.Tethering.PROGRAM_DIR),
            programRunId.getRun()));
    programDir.mkdirs();
    try (OutputStream os = programDir.append(DistributedProgramRunner.LOGBACK_FILE_NAME)
        .getOutputStream()) {
      os.write(
          files.get(DistributedProgramRunner.LOGBACK_FILE_NAME).getBytes(StandardCharsets.UTF_8));
    }
    try (OutputStream os = programDir.append(DistributedProgramRunner.TETHER_CONF_FILE_NAME)
        .getOutputStream()) {
      cConfCopy.writeXml(os);
    }
    if (files.containsKey(Constants.Security.Authentication.RUNTIME_TOKEN_FILE)) {
      try (OutputStream os = programDir
          .append(Constants.Security.Authentication.RUNTIME_TOKEN_FILE).getOutputStream()) {
        os.write(files.get(Constants.Security.Authentication.RUNTIME_TOKEN_FILE)
            .getBytes(StandardCharsets.UTF_8));
      }
    }

    Map<String, String> systemArgs = new HashMap<>(programOpts.getArguments().asMap());
    // Remove the plugin artifact archive argument from options and let the program runner recreate it
    systemArgs.remove(ProgramOptionConstants.PLUGIN_ARCHIVE);
    systemArgs.put(ProgramOptionConstants.PEER_NAME, peerInfo.getName());
    systemArgs.put(ProgramOptionConstants.PEER_ENDPOINT, peerInfo.getEndpoint());
    systemArgs.put(ProgramOptionConstants.RUNTIME_NAMESPACE, message.getRuntimeNamespace());
    systemArgs.put(ProgramOptionConstants.PROGRAM_RESOURCE_URI, programDir.toURI().toString());
    systemArgs.put(ProgramOptionConstants.CLUSTER_MODE, ClusterMode.ISOLATED.name());
    SystemArguments.addProfileArgs(systemArgs, Profile.NATIVE);

    // add namespace configs, which can contain things like the k8s namespace to execute in.
    Exception namespaceLookupFailure = null;
    try {
      NamespaceMeta namespaceMeta = namespaceQueryAdmin.get(
          new NamespaceId(message.getRuntimeNamespace()));
      SystemArguments.addNamespaceConfigs(systemArgs, namespaceMeta.getConfig());
    } catch (Exception e) {
      // can happen if the namespace doesn't exist for some reason, or for transient issues.
      namespaceLookupFailure = e;
    }

    ProgramOptions updatedOpts = new SimpleProgramOptions(programId, new BasicArguments(systemArgs),
        programOpts.getUserArguments());
    // Even if there was a failure earlier, need to send a provisioning message to make sure the run record is added.
    // Then send a failed message to fail the run.
    provisionerNotifier.provisioning(programRunId, updatedOpts, programDescriptor, "");
    LOG.debug("Published program start message for program run {}", programRunId);
    // Check if the peer is allowed to run programs on the namespace specified in the launch message
    // We first publish the PROVISIONING message to ensure that the run record is created.
    if (peerInfo.getMetadata().getNamespaceAllocations().stream()
        .noneMatch(n -> n.getNamespace().equals(message.getRuntimeNamespace()))) {
      LOG.warn("Namespace {} is not in the peer namespaces for peer {}",
          message.getRuntimeNamespace(), peerInfo.getName());
      programStateWriter.error(programRunId,
          new IllegalArgumentException(String.format("Cannot run program on namespace %s",
              message.getRuntimeNamespace())));
    } else if (namespaceLookupFailure != null) {
      programStateWriter.error(programRunId, namespaceLookupFailure);
      LOG.debug("Published program failed message for run {}", programRunId.getRun());
    }
  }

  private void publishStopProgram(String stopPayload) {
    ProgramRunId programRunId = new ProgramRunId(GSON.fromJson(stopPayload, ProgramRunInfo.class));
    // Do a graceful stop without a timeout. ProgramStopSubscriberService on the tethered peer will send a kill if the
    // graceful shutdown time is exceeded.
    programStateWriter.stop(programRunId, Integer.MAX_VALUE);
    LOG.debug("Published program stop message for program run {}", programRunId);
  }

  private void publishKillProgram(String stopPayload) {
    ProgramRunId programRunId = new ProgramRunId(GSON.fromJson(stopPayload, ProgramRunInfo.class));
    // Kill the program
    programStateWriter.stop(programRunId, 0);
    LOG.debug("Published program kill message for program run {}", programRunId);
  }

  /**
   * Reads from per-peer topic and returns program status updates for the given tethered peer.
   */
  private ProgramStateUpdates getProgramStateUpdates(String peerName) {
    String topic = programStateTopicPrefix + peerName;
    String lastMessageId = subscriberState.getLastMessageIdSent(peerName);
    List<Notification> notifications = new ArrayList<>();
    try (CloseableIterator<Message> iterator = messageFetcher.fetch(
        NamespaceId.SYSTEM.getNamespace(),
        topic,
        programUpdateFetchSize,
        subscriberState.getLastMessageIdSent(peerName))) {
      while (iterator.hasNext()) {
        Message message = iterator.next();
        notifications.add(message.decodePayload(r -> GSON.fromJson(r, Notification.class)));
        lastMessageId = message.getId();
      }
    } catch (TopicNotFoundException e) {
      LOG.error("Topic {} does not exist", topic);
    } catch (IOException e) {
      LOG.error("Exception when fetching program updates from per-peer topic at message id {}",
          subscriberState.getLastMessageIdSent(peerName), e);
    }
    return new ProgramStateUpdates(notifications, lastMessageId);
  }

  /**
   * Program status notifications for tethered peers.
   */
  private static class ProgramStateUpdates {

    // List of notifications
    private final List<Notification> notifications;
    // Last message id read from TMS
    private final String lastMessageId;

    private ProgramStateUpdates(List<Notification> notifications, String lastMessageId) {
      this.notifications = notifications;
      this.lastMessageId = lastMessageId;
    }
  }
}
