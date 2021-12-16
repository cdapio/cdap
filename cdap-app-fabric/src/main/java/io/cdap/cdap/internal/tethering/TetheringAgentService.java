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

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteAuthenticator;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
import javax.inject.Inject;

/**
 * The main class to run the remote agent service.
 */
public class TetheringAgentService extends AbstractRetryableScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(TetheringAgentService.class);
  private static final Gson GSON = new Gson();
  private static final String CONNECT_CONTROL_CHANNEL = "/v3/tethering/controlchannels/";
  private static final String SUBSCRIBER = "tether.agent";

  private final CConfiguration cConf;
  private final long connectionInterval;
  private final TetheringStore store;
  private final String instanceName;
  private final TransactionRunner transactionRunner;
  private final Map<String, String> lastMessageIds;

  @Inject
  TetheringAgentService(CConfiguration cConf, TransactionRunner transactionRunner, TetheringStore store) {
    super(RetryStrategies.fromConfiguration(cConf, "tethering.agent."));
    this.connectionInterval = TimeUnit.SECONDS.toMillis(cConf.getLong(Constants.Tethering.CONNECTION_INTERVAL));
    this.cConf = cConf;
    this.transactionRunner = transactionRunner;
    this.store = store;
    this.instanceName = cConf.get(Constants.INSTANCE_NAME);
    this.lastMessageIds = new HashMap<>();
  }

  @Override
  protected void doStartUp() throws InstantiationException, IllegalAccessException {
    Class<? extends RemoteAuthenticator> authClass = cConf.getClass(Constants.Tethering.CLIENT_AUTHENTICATOR_CLASS,
                                                                    null,
                                                                    RemoteAuthenticator.class);
    if (authClass != null) {
      RemoteAuthenticator.setDefaultAuthenticator(authClass.newInstance());
    }
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
        HttpResponse resp = TetheringUtils.sendHttpRequest(HttpMethod.GET, new URI(peer.getEndpoint())
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
      peerInfo.getMetadata().getNamespaceAllocations());
    try {
      URI endpoint = new URI(peerInfo.getEndpoint()).resolve(TetheringClientHandler.CREATE_TETHER + instanceName);
      HttpResponse response = TetheringUtils.sendHttpRequest(HttpMethod.PUT,
                                                             endpoint,
                                                             GSON.toJson(tetherRequest));
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
    processTetherControlResponse(resp.getResponseBodyAsString(StandardCharsets.UTF_8), peerInfo);
  }

  private void processTetherControlResponse(String message, PeerInfo peerInfo) {
    TetheringControlResponse[] responses = GSON.fromJson(message, TetheringControlResponse[].class);
    for (TetheringControlResponse response : responses) {
      TetheringControlMessage controlMessage = response.getControlMessage();
      switch (controlMessage.getType()) {
        case KEEPALIVE:
          LOG.trace("Got keepalive from {}", peerInfo.getName());
          break;
        case RUN_PIPELINE:
          // TODO: add processing logic
          break;
      }
    }

    if (responses.length > 0) {
      String lastMessageId = responses[responses.length - 1].getLastMessageId();
      lastMessageIds.put(peerInfo.getName(), lastMessageId);
    }
  }
}
