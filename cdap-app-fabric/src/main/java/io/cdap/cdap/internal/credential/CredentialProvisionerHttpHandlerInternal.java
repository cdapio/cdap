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

package io.cdap.cdap.internal.credential;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.NamespaceResourceReference;
import io.cdap.cdap.api.security.credential.CredentialProvisionerService;
import io.cdap.cdap.api.security.credential.CredentialProvisioningException;
import io.cdap.cdap.api.security.credential.ProvisionedCredential;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Internal {@link HttpHandler} for credential provisioning.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3)
public class CredentialProvisionerHttpHandlerInternal extends AbstractHttpHandler {
  private static final Gson GSON = new Gson();

  private final CredentialProvisionerService credentialProvisionerService;

  @Inject
  @VisibleForTesting
  public CredentialProvisionerHttpHandlerInternal(
      CredentialProvisionerService credentialProvisionerService) {
    this.credentialProvisionerService = credentialProvisionerService;
  }

  @POST
  @Path("/namespaces/{namespace-id}/credentials/identities/{identity-id}/provision")
  public void provisionCredential(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace, @PathParam("identity-id") String identity) {
    try {
      ProvisionedCredential credential = credentialProvisionerService
          .provision(new NamespaceResourceReference(namespace, identity));
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(credential));
    } catch (CredentialProvisioningException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
          String.format("Failed to provision credential '{}' for namespace '{}'",
              namespace, identity));
    }
  }
}
