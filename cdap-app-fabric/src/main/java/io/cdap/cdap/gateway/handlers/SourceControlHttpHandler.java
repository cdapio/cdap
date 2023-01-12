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

package io.cdap.cdap.gateway.handlers;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link io.cdap.http.HttpHandler} for source control management.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/repository")
public class SourceControlHttpHandler extends AbstractAppFabricHttpHandler {
  private final NamespaceAdmin namespaceAdmin;

  @Inject
  SourceControlHttpHandler(NamespaceAdmin namespaceAdmin) {
    this.namespaceAdmin = namespaceAdmin;
  }

  @DELETE
  @Path("/")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void deleteNamespaceRepository(FullHttpRequest request, HttpResponder responder,
                                        @PathParam("namespace-id") String namespaceId) throws Exception {
    namespaceAdmin.deleteRepository(new NamespaceId(namespaceId));
    responder.sendString(HttpResponseStatus.OK, String.format("Deleted repository configuration for namespace '%s'.",
                                                              namespaceId));
  }
}
