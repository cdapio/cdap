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

package io.cdap.cdap.internal.namespace.credential.handler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Singleton;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.worker.sidecar.GcpTokenResponse;
import io.cdap.cdap.internal.namespace.credential.NamespaceCredentialProviderService;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.credential.CredentialProvisioningException;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.security.auth.P4saTokenEndpointAuth;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal {@link HttpHandler} for credential providers.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3)
public class GcpWorkloadIdentityHttpHandlerInternal extends AbstractHttpHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(GcpWorkloadIdentityHttpHandlerInternal.class);
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(
      BasicThrowable.class, new BasicThrowableCodec()).create();

  private final CConfiguration cConf;
  private final NamespaceCredentialProviderService credentialProvider;

  @Inject
  public GcpWorkloadIdentityHttpHandlerInternal(CConfiguration cConf,
      NamespaceCredentialProviderService credentialProvider) {
    this.cConf = cConf;
    this.credentialProvider = credentialProvider;
  }

  /**
   * Provisions a credential for a given identity.
   *
   * @param request      The HTTP request.
   * @param responder    The HTTP responder.
   * @param namespace    The namespace of the identity for which to provision a credential.
   * @param scopes       A comma separated list of OAuth scopes requested.
   * @throws CredentialProvisioningException If provisioning fails.
   * @throws IOException                     If transport errors occur.
   * @throws NotFoundException               If the identity or associated profile are not found.
   */
  @GET
  @Path("/namespaces/{namespace-id}/credentials/workloadIdentity/provision")
  public void provisionCredential(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace, @QueryParam("scopes") String scopes)
      throws CredentialProvisioningException, IOException, NotFoundException {
    // If query params are updated, update the pattern matching URI at
    // io.cdap.cdap.common.encryption.EncryptionExemptionHook
    try {
      responder.sendJson(HttpResponseStatus.OK,
          GSON.toJson(credentialProvider.provision(namespace, scopes)));
    } catch (io.cdap.cdap.proto.credential.NotFoundException e) {
      throw new NotFoundException(e.getMessage());
    }
  }

  /**
   * Provisions a short-lived GCP access token for the system P4SA after verifying the constant-time
   * HMAC header {@link P4saTokenEndpointAuth#HEADER_NAME}.
   */
  @GET
  @Path("/system/p4sa/token")
  public void getSystemP4saToken(HttpRequest request, HttpResponder responder,
      @Nullable @QueryParam("scopes") String scopes)
      throws CredentialProvisioningException, IOException, NotFoundException {
    String token = request.headers().get(P4saTokenEndpointAuth.HEADER_NAME);
    if (!P4saTokenEndpointAuth.verifyToken(cConf, token)) {
      LOG.warn("Rejected unauthorized system P4SA token request on {}", request.uri());
      responder.sendString(HttpResponseStatus.FORBIDDEN,
          "Forbidden: invalid or missing system P4SA token.");
      return;
    }
    try {
      ProvisionedCredential credential = credentialProvider.provisionSystemP4sa(scopes);
      long expiresInSecs = Math.max(
          0L, Duration.between(Instant.now(), credential.getExpiration()).getSeconds());
      GcpTokenResponse response = new GcpTokenResponse("Bearer", credential.get(), expiresInSecs);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response));
    } catch (io.cdap.cdap.proto.credential.NotFoundException e) {
      throw new NotFoundException(e.getMessage());
    }
  }
}
