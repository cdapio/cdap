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
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedProgramRunner;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.tethering.proto.v1.TetheringLaunchMessage;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import javax.inject.Inject;
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
  private static final String CONNECT_CONTROL_CHANNEL = "/v3/tethering/controlchannels/";
  private static final String SUBSCRIBER = "tether.agent";

  public static final String REMOTE_TETHERING_AUTHENTICATOR = "remoteTetheringAuthenticator";

  private final CConfiguration cConf;
  private final long connectionInterval;
  private final TetheringStore store;
  private final String instanceName;
  private final TransactionRunner transactionRunner;
  private final Map<String, String> lastMessageIds;
  private final ProgramStateWriter programStateWriter;
  private final RemoteAuthenticator remoteAuthenticator;

  @Inject
  TetheringAgentService(CConfiguration cConf, TransactionRunner transactionRunner, TetheringStore store,
                        ProgramStateWriter programStateWriter,
                        @Named(REMOTE_TETHERING_AUTHENTICATOR) RemoteAuthenticator remoteAuthenticator) {
    super(RetryStrategies.fromConfiguration(cConf, "tethering.agent."));
    this.connectionInterval = TimeUnit.SECONDS.toMillis(cConf.getLong(Constants.Tethering.CONNECTION_INTERVAL, 10L));
    this.cConf = cConf;
    this.transactionRunner = transactionRunner;
    this.store = store;
    this.instanceName = cConf.get(Constants.INSTANCE_NAME);
    this.lastMessageIds = new HashMap<>();
    this.programStateWriter = programStateWriter;
    this.remoteAuthenticator = remoteAuthenticator;
  }

  @Override
  protected void doStartUp() {
    initializeMessageIds();
  }

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
    for (PeerInfo peer : peers) {
      try {
        Preconditions.checkArgument(peer.getEndpoint() != null,
                                    "Peer %s doesn't have an endpoint", peer.getName());
        String uri = CONNECT_CONTROL_CHANNEL + instanceName;
        String lastMessageId = lastMessageIds.get(peer);
        if (lastMessageId != null) {
          uri = uri + "?messageId=" + URLEncoder.encode(lastMessageId, "UTF-8");
        }
        HttpResponse resp = TetheringUtils.sendHttpRequest(remoteAuthenticator, HttpMethod.GET,
                                                           new URI(peer.getEndpoint())
                                                             .resolve(uri));
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
      for (Map.Entry<String, String> entry : lastMessageIds.entrySet()) {
        appMetadataStore.persistSubscriberState(entry.getKey(), SUBSCRIBER, entry.getValue());
      }
    });

    return connectionInterval;
  }

  private void handleNotFound(PeerInfo peerInfo) throws IOException {
    // Update last connection timestamp.
    store.updatePeerTimestamp(peerInfo.getName());

    TetheringConnectionRequest tetherRequest = new TetheringConnectionRequest(
      peerInfo.getMetadata().getNamespaceAllocations(), peerInfo.getRequestTime(),
      peerInfo.getMetadata().getDescription());
    try {
      URI endpoint = new URI(peerInfo.getEndpoint()).resolve(TetheringClientHandler.CREATE_TETHER + instanceName);
      HttpResponse response = TetheringUtils.sendHttpRequest(remoteAuthenticator, HttpMethod.PUT,
                                                             endpoint, GSON.toJson(tetherRequest));
      if (response.getResponseCode() != 200) {
        LOG.error("Failed to initiate tether with peer {}, response body: {}, code: {}",
                  peerInfo.getName(), response.getResponseBody(), response.getResponseCode());
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
        String messageId = appMetadataStore.retrieveSubscriberState(peer, SUBSCRIBER);
        lastMessageIds.put(peer, messageId);
      }
    });
  }

  private void handleForbidden(PeerInfo peerInfo) throws IOException {
    // Set tethering status to rejected.
    store.updatePeerStatusAndTimestamp(peerInfo.getName(), TetheringStatus.REJECTED);
  }

  private void handleResponse(HttpResponse resp, PeerInfo peerInfo) throws IOException {
    if (peerInfo.getTetheringStatus() == TetheringStatus.PENDING) {
      LOG.debug("Peer {} transitioned to ACCEPTED state", peerInfo.getName());
      store.updatePeerStatusAndTimestamp(peerInfo.getName(), TetheringStatus.ACCEPTED);
    } else {
      // Update last connection timestamp.
      store.updatePeerTimestamp(peerInfo.getName());
    }
    processTetheringControlResponse(resp.getResponseBodyAsString(StandardCharsets.UTF_8), peerInfo);
  }

  private void processTetheringControlResponse(String message, PeerInfo peerInfo) throws IOException {
    TetheringControlResponse[] responses = GSON.fromJson(message, TetheringControlResponse[].class);
    for (TetheringControlResponse response : responses) {
      TetheringControlMessage controlMessage = response.getControlMessage();
      switch (controlMessage.getType()) {
        case KEEPALIVE:
          LOG.trace("Got keepalive from {}", peerInfo.getName());
          break;
        case START_PROGRAM:
          LOG.trace("Got start_program from {}", peerInfo.getName());
          publishStartProgram(Bytes.toString(controlMessage.getPayload()), peerInfo.getName());
          break;
        case STOP_PROGRAM:
          LOG.trace("Got stop_program from {}", peerInfo.getName());
          publishStopProgram(Bytes.toString(controlMessage.getPayload()));
          break;
      }
    }

    if (responses.length > 0) {
      String lastMessageId = responses[responses.length - 1].getLastMessageId();
      lastMessageIds.put(peerInfo.getName(), lastMessageId);
    }
  }

  /**
   * Modify received files to reference the namespace in this instance and to include the peer name for RunRecord.
   * Use these files to start the program through ProgramStateWriter.
   * Save the rest of the files into a temp dir (to be loaded when the Program is created in DistributedProgramRunner).
   */
  private void publishStartProgram(String runPayload, String peerName) throws IOException {
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
    ProgramId programId = new ProgramId(message.getNamespace(), programOpts.getProgramId().getApplication(),
                                        programOpts.getProgramId().getType(), programOpts.getProgramId().getProgram());
    Map<String, String> systemArgs = new HashMap<>(programOpts.getArguments().asMap());
    systemArgs.put(ProgramOptionConstants.PEER_NAME, peerName);
    ProgramOptions updatedOpts = new SimpleProgramOptions(programId, new BasicArguments(systemArgs),
                                                          programOpts.getUserArguments());
    ProgramRunId programRunId = programId.run(programOpts.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, appSpec);

    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    File runDir = new File(tmpDir, programRunId.getRun());
    runDir.mkdirs();
    Files.write(files.get(DistributedProgramRunner.LOGBACK_FILE_NAME),
                new File(runDir, DistributedProgramRunner.LOGBACK_FILE_NAME), StandardCharsets.UTF_8);
    try (Writer writer = Files.newWriter(new File(runDir, DistributedProgramRunner.TETHER_CONF_FILE_NAME),
                                         StandardCharsets.UTF_8)) {
      cConfCopy.writeXml(writer);
    }

    programStateWriter.start(programRunId, updatedOpts, null, programDescriptor);
    LOG.debug("Published program start message for run {}", programRunId.getRun());
  }

  private void publishStopProgram(String stopPayload) {
    ProgramRunInfo programRunInfo = GSON.fromJson(stopPayload, ProgramRunInfo.class);
    ProgramRunId programRunId = new ProgramRunId(programRunInfo.getNamespace(), programRunInfo.getApplication(),
                                                 ProgramType.valueOf(programRunInfo.getProgramType()),
                                                 programRunInfo.getProgram(), programRunInfo.getRun());
    programStateWriter.killed(programRunId);
    LOG.debug("Published program stop message for run {}", programRunInfo.getRun());
  }
}
