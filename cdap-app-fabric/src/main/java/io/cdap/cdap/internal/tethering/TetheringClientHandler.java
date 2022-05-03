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
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.InstancePermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * {@link io.cdap.http.HttpHandler} to manage tethering client v3 REST APIs
 */
@Path(Constants.Gateway.API_VERSION_3)
public class TetheringClientHandler extends AbstractHttpHandler {
  private static final Gson GSON = new Gson();

  private final TetheringStore store;
  private final ContextAccessEnforcer contextAccessEnforcer;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  TetheringClientHandler(TetheringStore store, ContextAccessEnforcer contextAccessEnforcer,
                         NamespaceQueryAdmin namespaceQueryAdmin) {
    this.store = store;
    this.contextAccessEnforcer = contextAccessEnforcer;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  /**
   * Initiates tethering with the server.
   */
  @PUT
  @Path("/tethering/create")
  public void createTethering(FullHttpRequest request, HttpResponder responder) throws Exception {
    contextAccessEnforcer.enforce(InstanceId.SELF, InstancePermission.TETHER);
    String content = request.content().toString(StandardCharsets.UTF_8);
    TetheringCreationRequest tetheringCreationRequest = GSON.fromJson(content, TetheringCreationRequest.class);
    List<NamespaceAllocation> namespaces = tetheringCreationRequest.getNamespaceAllocations();
    validateNamespaces(namespaces);
    PeerMetadata peerMetadata = new PeerMetadata(namespaces, tetheringCreationRequest.getMetadata(),
                                                 tetheringCreationRequest.getDescription());
    PeerInfo peerInfo = new PeerInfo(tetheringCreationRequest.getPeer(), tetheringCreationRequest.getEndpoint(),
                                     TetheringStatus.PENDING, peerMetadata, System.currentTimeMillis());
    store.addPeer(peerInfo);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private void validateNamespaces(List<NamespaceAllocation> namespaceAllocations) throws Exception {
    for (NamespaceAllocation namespaceAllocation : namespaceAllocations) {
      NamespaceId namespaceId = new NamespaceId(namespaceAllocation.getNamespace());
      if (!namespaceQueryAdmin.exists(namespaceId)) {
        throw new NamespaceNotFoundException(namespaceId);
      }
    }
  }
}
