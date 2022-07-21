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

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.NotFoundException;
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
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeProgramStatusSubscriberService;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.provision.ProvisionerNotifier;
import io.cdap.cdap.internal.tethering.proto.v1.TetheringLaunchMessage;
import io.cdap.cdap.logging.gateway.handlers.ProgramRunRecordFetcher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import org.apache.commons.io.IOUtils;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import javax.inject.Named;

/**
 * The main class to run the remote agent service.
 */
public class TetheringAgentService extends AbstractRetryableScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(TetheringAgentService.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();
  private static final String CREATE_TETHER = "v3/tethering/connections/";
  private static final String CONNECT_CONTROL_CHANNEL = "v3/tethering/controlchannels/";
  private static final String SUBSCRIBER = "tether.agent";
  private static final String PEER_TOPIC_PREFIX = "tethering.peer.message.state.";

  public static final String REMOTE_TETHERING_AUTHENTICATOR = "remoteTetheringAuthenticator";

  private final CConfiguration cConf;
  private final long connectionInterval;
  private final TetheringStore store;
  private final String instanceName;
  private final TransactionRunner transactionRunner;
  // Used by TetheringServerHandler to track last TetheringControlMessage sent to this agent
  private final Map<String, String> peerToLastControlMessageIds;
  private final ProgramStateWriter programStateWriter;
  private final MessageFetcher messageFetcher;
  private final ProgramRunRecordFetcher runRecordFetcher;
  private final RemoteAuthenticator remoteAuthenticator;
  private final LocationFactory locationFactory;
  private final ProvisionerNotifier provisionerNotifier;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final String programUpdateTopic;
  private final int programUpdateFetchSize;
  private String lastProgramUpdateMessageId;
  // Tracks last unsent message. Used to update the value of lastProgramUpdateMessageId after successful HTTP request
  private String lastUnsentProgramUpdateMessageId;

  @Inject
  TetheringAgentService(CConfiguration cConf, TransactionRunner transactionRunner, TetheringStore store,
                        ProgramStateWriter programStateWriter, MessagingService messagingService,
                        ProgramRunRecordFetcher programRunRecordFetcher,
                        @Named(REMOTE_TETHERING_AUTHENTICATOR) RemoteAuthenticator remoteAuthenticator,
                        LocationFactory locationFactory,
                        ProvisionerNotifier provisionerNotifier,
                        NamespaceQueryAdmin namespaceQueryAdmin) {
    super(RetryStrategies.fromConfiguration(cConf, "tethering.agent."));
    this.connectionInterval = TimeUnit.SECONDS.toMillis(cConf.getLong(Constants.Tethering.CONNECTION_INTERVAL, 10L));
    this.cConf = cConf;
    this.transactionRunner = transactionRunner;
    this.store = store;
    this.instanceName = cConf.get(Constants.INSTANCE_NAME);
    this.peerToLastControlMessageIds = new HashMap<>();
    this.programStateWriter = programStateWriter;
    this.messageFetcher = new MultiThreadMessagingContext(messagingService).getMessageFetcher();
    this.runRecordFetcher = programRunRecordFetcher;
    this.remoteAuthenticator = remoteAuthenticator;
    this.locationFactory = locationFactory;
    this.provisionerNotifier = provisionerNotifier;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.programUpdateTopic = cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC);
    this.programUpdateFetchSize = cConf.getInt(Constants.AppFabric.STATUS_EVENT_FETCH_SIZE);
  }

  @Override
  protected void doStartUp() {
    initializeMessageIds();
  }

  /**
   * Creates a map between peered instances and the list of Notifications to be sent over. Then connects to the control
   * channel and processes the TetheringControlResponses.
   */
  @Override
  protected long runTask() {
    List<PeerInfo> peers;
    try {
      peers = store.getPeers().stream()
        // Ignore peers in REJECTED state.
        .filter(p -> p.getTetheringStatus() != TetheringStatus.REJECTED)
        .collect(Collectors.toList());
    } catch (IOException e) {
      LOG.warn("Failed to get peer information", e);
      return connectionInterval;
    }
    Map<String, List<Notification>> peerToNotifications = getPeerProgramUpdates();
    for (PeerInfo peer : peers) {
      try {
        Preconditions.checkArgument(peer.getEndpoint() != null,
                                    "Peer %s doesn't have an endpoint", peer.getName());
        String lastControlMessageId = peerToLastControlMessageIds.get(peer.getName());
        List<Notification> notificationList = peerToNotifications.get(peer.getName());
        String content = GSON.toJson(new TetheringControlChannelRequest(lastControlMessageId, notificationList));
        String path = appendPath(Objects.requireNonNull(peer.getEndpoint()), CONNECT_CONTROL_CHANNEL + instanceName);
        URI peerUri = new URI(peer.getEndpoint()).resolve(path);
        HttpResponse resp = TetheringUtils.sendHttpRequest(remoteAuthenticator, HttpMethod.POST,
                                                           peerUri, content);
        switch (resp.getResponseCode()) {
          case HttpURLConnection.HTTP_OK:
            handleResponse(resp, peer);
            break;
          case HttpURLConnection.HTTP_NOT_FOUND:
            handleNotFound(peer);
            break;
          case HttpURLConnection.HTTP_FORBIDDEN:
            handleForbidden(peer);
            break;
          default:
            LOG.error("Peer {} returned unexpected error code {} body {}",
                      peer.getName(), resp.getResponseCode(),
                      resp.getResponseBodyAsString(StandardCharsets.UTF_8));
        }
      } catch (Exception e) {
        LOG.debug("Failed to create control channel to {}", peer, e);
      }
    }

    // Update last message ids in the store for all peers
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
      for (Map.Entry<String, String> entry : peerToLastControlMessageIds.entrySet()) {
        appMetadataStore.persistSubscriberState(PEER_TOPIC_PREFIX + entry.getKey(), SUBSCRIBER, entry.getValue());
      }
      lastProgramUpdateMessageId = lastUnsentProgramUpdateMessageId;
      appMetadataStore.persistSubscriberState(programUpdateTopic, SUBSCRIBER, lastProgramUpdateMessageId);
    });

    return connectionInterval;
  }

  private String appendPath(String base, String path) {
    return base.endsWith("/") ? base + path : base + '/' + path;
  }

  private void handleNotFound(PeerInfo peerInfo) throws IOException {
    // Update last connection timestamp.
    store.updatePeerTimestamp(peerInfo.getName());

    TetheringConnectionRequest tetherRequest = new TetheringConnectionRequest(
      peerInfo.getMetadata().getNamespaceAllocations(), peerInfo.getRequestTime(),
      peerInfo.getMetadata().getDescription());
    try {
      String path = appendPath(Objects.requireNonNull(peerInfo.getEndpoint()), CREATE_TETHER + instanceName);
      URI endpoint = new URI(peerInfo.getEndpoint()).resolve(path);
      HttpResponse response = TetheringUtils.sendHttpRequest(remoteAuthenticator, HttpMethod.PUT,
                                                             endpoint, GSON.toJson(tetherRequest));
      if (response.getResponseCode() != 200) {
        LOG.error("Failed to initiate tether with peer {}, response body: {}, code: {}",
                  peerInfo.getName(), response.getResponseBodyAsString(StandardCharsets.UTF_8),
                  response.getResponseCode());
      }
    } catch (URISyntaxException | IOException e) {
      LOG.error("Failed to send tether request to peer {}, endpoint {}",
                peerInfo.getName(), peerInfo.getEndpoint());
    }
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

    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
      for (String peer : peers) {
        String messageId = appMetadataStore.retrieveSubscriberState(PEER_TOPIC_PREFIX + peer, SUBSCRIBER);
        peerToLastControlMessageIds.put(peer, messageId);
      }
      // Initialize subscriber based on last message fetched by RuntimeProgramStatusSubscriberService.
      // This is to avoid fetching existing program history from before TetheringAgent was added
      lastProgramUpdateMessageId = appMetadataStore.retrieveSubscriberState(programUpdateTopic, SUBSCRIBER);
      if (lastProgramUpdateMessageId == null) {
        String messageId = appMetadataStore.retrieveSubscriberState(programUpdateTopic,
                                                                    RuntimeProgramStatusSubscriberService.SUBSCRIBER);
        appMetadataStore.persistSubscriberState(programUpdateTopic, SUBSCRIBER, messageId);
        lastProgramUpdateMessageId = messageId;
      }
    });
  }

  private void handleForbidden(PeerInfo peerInfo) throws IOException {
    // Set tethering status to rejected.
    store.updatePeerStatusAndTimestamp(peerInfo.getName(), TetheringStatus.REJECTED);
  }

  private void handleResponse(HttpResponse resp, PeerInfo peerInfo) throws IOException {
    TetheringControlResponse[] responses;
    try {
      responses = GSON.fromJson(resp.getResponseBodyAsString(StandardCharsets.UTF_8), TetheringControlResponse[].class);
    } catch (Exception e) {
      LOG.error("Failed to parse response from peer {}: {}",
                peerInfo.getName(),
                resp.getResponseBodyAsString(StandardCharsets.UTF_8), e);
      return;
    }
    if (peerInfo.getTetheringStatus() == TetheringStatus.PENDING) {
      LOG.debug("Peer {} transitioned to ACCEPTED state", peerInfo.getName());
      store.updatePeerStatusAndTimestamp(peerInfo.getName(), TetheringStatus.ACCEPTED);
    } else {
      // Update last connection timestamp.
      store.updatePeerTimestamp(peerInfo.getName());
    }
    processTetheringControlResponse(responses, peerInfo);
  }

  private void processTetheringControlResponse(TetheringControlResponse[] responses, PeerInfo peerInfo)
    throws IOException {
    for (TetheringControlResponse response : responses) {
      TetheringControlMessage controlMessage = response.getControlMessage();
      switch (controlMessage.getType()) {
        case KEEPALIVE:
          LOG.trace("Got keepalive from {}", peerInfo.getName());
          break;
        case START_PROGRAM:
          LOG.trace("Got start_program from {}", peerInfo.getName());
          publishStartProgram(Bytes.toString(controlMessage.getPayload()), peerInfo);
          break;
        case STOP_PROGRAM:
          LOG.trace("Got stop_program from {}", peerInfo.getName());
          publishStopProgram(Bytes.toString(controlMessage.getPayload()));
          break;
        case KILL_PROGRAM:
          LOG.trace("Got kill_program from {}", peerInfo.getName());
          publishKillProgram(Bytes.toString(controlMessage.getPayload()));
          break;
      }
    }

    if (responses.length > 0) {
      String lastMessageId = responses[responses.length - 1].getLastMessageId();
      peerToLastControlMessageIds.put(peerInfo.getName(), lastMessageId);
    }
  }

  /**
   * Modify received files to reference the namespace in this instance and to include the peer name for RunRecord.
   * Use these files to start the program through ProgramStateWriter.
   * Save the rest of the files into a temp dir (to be loaded when the Program is created in DistributedProgramRunner).
   */
  private void publishStartProgram(String runPayload, PeerInfo peerInfo) throws IOException {
    TetheringLaunchMessage message = GSON.fromJson(runPayload, TetheringLaunchMessage.class);

    CConfiguration cConfCopy = CConfiguration.copy(cConf);
    for (Map.Entry<String, String> cConfEntry : message.getCConfEntries().entrySet()) {
      cConfCopy.set(cConfEntry.getKey(), cConfEntry.getValue());
    }

    Map<String, String> files = new HashMap<>();
    for (Map.Entry<String, byte[]> file : message.getFiles().entrySet()) {
      try (GZIPInputStream inputStream = new GZIPInputStream(new ByteArrayInputStream(file.getValue()))) {
        files.put(file.getKey(), IOUtils.toString(inputStream));
      }
    }

    ApplicationSpecification appSpec = GSON.fromJson(files.get(DistributedProgramRunner.APP_SPEC_FILE_NAME),
                                                     ApplicationSpecification.class);
    ProgramOptions programOpts = GSON.fromJson(files.get(DistributedProgramRunner.PROGRAM_OPTIONS_FILE_NAME),
                                               ProgramOptions.class);
    ProgramId programId = programOpts.getProgramId();
    ProgramRunId programRunId = programId.run(programOpts.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, appSpec);

    Location programDir = locationFactory.create(String.format("%s/%s", cConf.get(Constants.Tethering.PROGRAM_DIR),
                                                               programRunId.getRun()));
    programDir.mkdirs();
    try (OutputStream os = programDir.append(DistributedProgramRunner.LOGBACK_FILE_NAME).getOutputStream()) {
      os.write(files.get(DistributedProgramRunner.LOGBACK_FILE_NAME).getBytes(StandardCharsets.UTF_8));
    }
    try (OutputStream os = programDir.append(DistributedProgramRunner.TETHER_CONF_FILE_NAME).getOutputStream()) {
      cConfCopy.writeXml(os);
    }
    if (files.containsKey(Constants.Security.Authentication.RUNTIME_TOKEN_FILE)) {
      try (OutputStream os = programDir
        .append(Constants.Security.Authentication.RUNTIME_TOKEN_FILE).getOutputStream()) {
        os.write(files.get(Constants.Security.Authentication.RUNTIME_TOKEN_FILE).getBytes(StandardCharsets.UTF_8));
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
      NamespaceMeta namespaceMeta = namespaceQueryAdmin.get(new NamespaceId(message.getRuntimeNamespace()));
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
    ProgramRunInfo programRunInfo = GSON.fromJson(stopPayload, ProgramRunInfo.class);
    ProgramRunId programRunId = new ProgramRunId(programRunInfo.getNamespace(), programRunInfo.getApplication(),
                                                 ProgramType.valueOf(programRunInfo.getProgramType()),
                                                 programRunInfo.getProgram(), programRunInfo.getRun());
    // Do a graceful stop without a timeout. ProgramStopSubscriberService on the tethered peer will send a kill if the
    // graceful shutdown time is exceeded.
    programStateWriter.stop(programRunId, Integer.MAX_VALUE);
    LOG.debug("Published program stop message for program run {}", programRunId);
  }

  private void publishKillProgram(String stopPayload) {
    ProgramRunInfo programRunInfo = GSON.fromJson(stopPayload, ProgramRunInfo.class);
    ProgramRunId programRunId = new ProgramRunId(programRunInfo.getNamespace(), programRunInfo.getApplication(),
                                                 ProgramType.valueOf(programRunInfo.getProgramType()),
                                                 programRunInfo.getProgram(), programRunInfo.getRun());
    // Kill the program
    programStateWriter.stop(programRunId, 0);
    LOG.debug("Published program kill message for program run {}", programRunId);
  }

  /**
   * Add notification to map if associated by a tethered peer
   */
  private Map<String, List<Notification>> getPeerProgramUpdates() {
    Map<String, List<Notification>> peerToNotifications = new HashMap<>();
    lastUnsentProgramUpdateMessageId = lastProgramUpdateMessageId;
    try (CloseableIterator<Message> iterator = messageFetcher.fetch(NamespaceId.SYSTEM.getNamespace(),
                                                                    programUpdateTopic,
                                                                    programUpdateFetchSize,
                                                                    lastProgramUpdateMessageId)) {
      while (iterator.hasNext()) {
        Message message = iterator.next();
        Notification notification = message.decodePayload(r -> GSON.fromJson(r, Notification.class));
        if (notification.getNotificationType() == Notification.Type.PROGRAM_STATUS) {
          Map<String, String> properties = notification.getProperties();
          String programRunId = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
          try {
            RunRecord runRecord = runRecordFetcher.getRunRecordMeta(GSON.fromJson(programRunId, ProgramRunId.class));
            if (runRecord.getPeerName() != null) {
              peerToNotifications.computeIfAbsent(runRecord.getPeerName(), n -> new ArrayList<>()).add(notification);
            }
          } catch (NotFoundException | IOException e) {
            LOG.error("Unable to fetch runRecord for programRunId {}", programRunId, e);
          }
        }
        lastUnsentProgramUpdateMessageId = message.getId();
      }
    } catch (Exception e) {
      LOG.error("Exception when fetching program updates. Will retry again during next poll", e);
    }
    return peerToNotifications;
  }
}
