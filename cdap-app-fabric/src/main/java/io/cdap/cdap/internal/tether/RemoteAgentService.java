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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;

/**
 * The main class to run the remote agent service.
 */
public class RemoteAgentService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteAgentService.class);
  private static final Gson GSON = new Gson();
  public static final String CONNECT_CONTROL_CHANNEL = "/v3/tethering/controlchannels/";

  private final CConfiguration cConf;
  private final TetherStore store;
  private String instanceName;
  private ScheduledExecutorService executorService;

  @Inject
  public RemoteAgentService(CConfiguration cConf, TransactionRunner transactionRunner) {
    this.cConf = cConf;
    this.store = new TetherStore(transactionRunner);
  }

  @Override
  protected void startUp() throws Exception {
    instanceName = cConf.get(Constants.INSTANCE_NAME);
    int connectionInterval = cConf.getInt(Constants.Tether.CONNECT_INTERVAL,
                                          Constants.Tether.CONNECT_INTERVAL_DEFAULT);
    executorService = Executors
      .newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("tether-control-channel"));
    executorService.scheduleWithFixedDelay(
      () -> {
        List<PeerInfo> peers = store.getPeers().stream()
          // Ignore peers in REJECTED state.
          .filter(p -> p.getTetherStatus() != TetherStatus.REJECTED)
          .collect(Collectors.toList());
        for (PeerInfo peer : peers) {
          if (peer.getEndpoint() == null) {
            // Should not happen
            LOG.error("Peer {} does not have endpoint set", peer.getName());
            continue;
          }
          try {
            HttpResponse resp = TetherUtils.sendHttpRequest(HttpMethod.GET, new URI(peer.getEndpoint())
              .resolve(CONNECT_CONTROL_CHANNEL + instanceName));
            switch (resp.getResponseCode()) {
              case 200:
                TetherStatus peerStatus = store.getTetherStatus(peer.getName());
                 if (peerStatus == TetherStatus.PENDING) {
                  LOG.info("Peer {} transitioned to ACCEPTED state", peer.getName());
                  store.updatePeer(peer.getName(), TetherStatus.ACCEPTED);
                }
                // Update last connection timestamp.
                store.updatePeer(peer.getName(), System.currentTimeMillis());
                processTetherControlMessage(resp.getResponseBodyAsString(StandardCharsets.UTF_8), peer);
                break;
              case 404:
                // Tether does not exist on the peer, create it.
                if (peer.getEndpoint() == null) {
                  LOG.warn("Peer {} endpoint is null", peer.getName());
                  break;
                }
                // Update last connection timestamp.
                store.updatePeer(peer.getName(), System.currentTimeMillis());

                TetherConnectionRequest tetherRequest = new TetherConnectionRequest(instanceName,
                                                                                    peer.getMetadata()
                                                                                      .getNamespaces());
                try {
                  HttpResponse response = TetherUtils.sendHttpRequest(HttpMethod.POST,
                                                                      new URI(peer.getEndpoint())
                                                                        .resolve(TetherClientHandler.CREATE_TETHER),
                                                                      GSON.toJson(tetherRequest));
                  if (response.getResponseCode() != 200) {
                    LOG.error("Failed to initiate tether with peer {}, response body: {}, code: {}",
                              peer.getName(), response.getResponseBody(), response.getResponseCode());
                  }
                } catch (URISyntaxException | IOException e) {
                  LOG.error("Failed to send tether request to peer {}, endpoint {}",
                            peer.getName(), peer.getEndpoint());
                }
                break;
              case 403:
                // Server rejected tethering
                TetherStatus tetherStatus = store.getTetherStatus(peer.getName());
                if (tetherStatus != TetherStatus.PENDING) {
                  if (tetherStatus != TetherStatus.REJECTED) {
                    LOG.info("Ignoring tethering rejection message from {}, current state: {}", peer.getName(),
                             store.getTetherStatus(peer.getName()));
                  }
                  break;
                }
                // Set tether status to rejected and update last connection timestamp.
                store.updatePeer(peer.getName(), TetherStatus.REJECTED, System.currentTimeMillis());
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
      }, connectionInterval, connectionInterval, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    executorService.shutdown();
  }

  private void processTetherControlMessage(String message, PeerInfo peerInfo) {
    Type type = new TypeToken<List<TetherControlMessage>>() { }.getType();
    List<TetherControlMessage> tetherControlMessages = GSON.fromJson(message, type);
    for (TetherControlMessage tetherControlMessage : tetherControlMessages) {
      switch (tetherControlMessage.getType()) {
        case KEEPALIVE:
          LOG.debug("Got keeplive from {}", peerInfo.getName());
          break;
        case RUN_PIPELINE:
          // TODO: add processing logic
          break;
      }
    }
  }
}
