/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * TODO
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class MyArtifactHttpHandler extends AbstractHttpHandler {

  /**
   * TODO
   */
  @GET
  @Path("/namespaces/{namespace-id}/work")
  public void getArtifacts(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) {

    responder.sendJson(HttpResponseStatus.OK, "WORK");
    // try {
    //   if (scope == null) {
    //     NamespaceId namespace = validateAndGetNamespace(namespaceId);
    //     responder.sendJson(HttpResponseStatus.OK,
    //                        GSON.toJson(artifactRepository.getArtifactSummaries(namespace, true)));
    //   } else {
    //     NamespaceId namespace = validateAndGetScopedNamespace(Ids.namespace(namespaceId), scope);
    //     responder.sendJson(HttpResponseStatus.OK,
    //                        GSON.toJson(artifactRepository.getArtifactSummaries(namespace, false)));
    //   }
    // } catch (IOException e) {
    //   LOG.error("Exception reading artifact metadata for namespace {} from the store.", namespaceId, e);
    //   responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error reading artifact metadata from
    // the store.");
    // }
  }

}
