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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * {@link io.cdap.http.HttpHandler} to manage tethering client v3 REST APIs
 */
@Path(Constants.Gateway.API_VERSION_3)
public class TetherClientHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TetherClientHandler.class);
  private static final Gson GSON = new Gson();
  static final String CREATE_TETHER = "/v3/tethering/connect";
  // Following constants are same as defined in KubeMasterEnvironment
  static final String NAMESPACE_CPU_LIMIT_PROPERTY = "k8s.namespace.cpu.limits";
  static final String NAMESPACE_MEMORY_LIMIT_PROPERTY = "k8s.namespace.memory.limits";

  private final TetherStore store;
  private final NamespaceAdmin namespaceAdmin;
  private final String instanceName;

  @Inject
  TetherClientHandler(CConfiguration cConf, TetherStore store, NamespaceAdmin namespaceAdmin) {
    this.store = store;
    this.namespaceAdmin = namespaceAdmin;
    this.instanceName = cConf.get(Constants.INSTANCE_NAME);
  }

  /**
   * Initiates tethering with the server.
   */
  @POST
  @Path("/tethering/create")
  public void createTether(FullHttpRequest request, HttpResponder responder) throws Exception {
    String content = request.content().toString(StandardCharsets.UTF_8);
    TetherCreationRequest tetherCreationRequest = GSON.fromJson(content, TetherCreationRequest.class);

    List<PeerInfo> peers = store.getPeers();
    Optional<PeerInfo> peer = peers.stream()
      .filter(p -> p.getName().equals(tetherCreationRequest.getPeer()))
      .findFirst();
    if (peer.isPresent()) {
      LOG.info("Peer {} is already present in state {}, ignoring tethering request",
               peer.get().getName(), peer.get().getTetherStatus());
      responder.sendStatus(HttpResponseStatus.OK);
      return;
    }

    List<NamespaceAllocation> namespaceAllocations = new ArrayList<>();
    for (String namespace : tetherCreationRequest.getNamespaces()) {
      Optional<NamespaceMeta> namespaceMeta = namespaceAdmin.list().stream()
      .filter(ns -> ns.getName().equals(namespace)).findFirst();
      if (!namespaceMeta.isPresent()) {
        LOG.error("Namespace {} does not exist", namespace);
        throw new IllegalArgumentException(String.format("Namespace {} does not exist", namespace));
      }
      String cpuLimit = namespaceMeta.get().getConfig().getConfig(NAMESPACE_CPU_LIMIT_PROPERTY);
      String memoryLimit = namespaceMeta.get().getConfig().getConfig(NAMESPACE_MEMORY_LIMIT_PROPERTY);
      namespaceAllocations.add(new NamespaceAllocation(namespace, cpuLimit, memoryLimit));
    }
    TetherConnectionRequest tetherConnectionRequest = new TetherConnectionRequest(instanceName,
                                                                                  namespaceAllocations);
    URI endpoint = new URI(tetherCreationRequest.getEndpoint());
    HttpResponse response = TetherUtils.sendHttpRequest(HttpMethod.POST, endpoint.resolve(CREATE_TETHER),
                                                        GSON.toJson(tetherConnectionRequest));
    if (response.getResponseCode() != 200) {
      LOG.error("Failed to send tether request, body: {}, code: {}",
                response.getResponseBody(), response.getResponseCode());
      responder.sendStatus(HttpResponseStatus.valueOf(response.getResponseCode()));
      return;
    }

    PeerMetadata peerMetadata = new PeerMetadata(namespaceAllocations, tetherCreationRequest.getMetadata());
    PeerInfo peerInfo = new PeerInfo(tetherCreationRequest.getPeer(), tetherCreationRequest.getEndpoint(),
                                     TetherStatus.PENDING, peerMetadata);
    store.addPeer(peerInfo);
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
