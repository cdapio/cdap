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

package io.cdap.cdap.internal.namespace.credential;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.retry.Idempotency;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.internal.credential.RemoteCredentialProvider;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.credential.CredentialProvisioningException;
import io.cdap.cdap.proto.credential.IdentityValidationException;
import io.cdap.cdap.proto.credential.NamespaceCredentialProvider;
import io.cdap.cdap.proto.credential.NamespaceWorkloadIdentity;
import io.cdap.cdap.proto.credential.NotFoundException;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import joptsimple.internal.Strings;

/**
 * Remote implementation for {@link NamespaceCredentialProvider} used in {@link
 * io.cdap.cdap.common.conf.Constants.ArtifactLocalizer}.
 */
public class RemoteNamespaceCredentialProvider implements NamespaceCredentialProvider {

  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
      new BasicThrowableCodec()).create();
  private final RemoteClient remoteClient;

  /**
   * Construct the {@link RemoteCredentialProvider}.
   *
   * @param remoteClientFactory   A factory to create {@link RemoteClient}.
   * @param internalAuthenticator An authenticator to propagate internal identity headers.
   */
  public RemoteNamespaceCredentialProvider(RemoteClientFactory remoteClientFactory,
      InternalAuthenticator internalAuthenticator) {

    this.remoteClient = remoteClientFactory.createRemoteClient(Constants.Service.APP_FABRIC_HTTP,
        RemoteClientFactory.NO_VERIFY_HTTP_REQUEST_CONFIG, Constants.Gateway.INTERNAL_API_VERSION_3,
        internalAuthenticator);
  }

  /**
   * Provisions a short-lived credential for the provided identity using the provided identity.
   *
   * @param namespace The identity namespace.
   * @param scopes    A comma separated list of OAuth scopes requested.
   * @return A short-lived credential.
   * @throws CredentialProvisioningException If provisioning the credential fails.
   * @throws IOException                     If any transport errors occur.
   * @throws NotFoundException               If the profile or identity are not found.
   */
  @Override
  public ProvisionedCredential provision(String namespace, String scopes)
      throws CredentialProvisioningException, IOException, NotFoundException {
    String url = String.format("namespaces/%s/credentials/workloadIdentity/provision",
        namespace);
    if (!Strings.isNullOrEmpty(scopes)) {
      url = String.format("%s?scopes=%s", url, scopes);
    }
    io.cdap.common.http.HttpRequest tokenRequest =
        remoteClient.requestBuilder(HttpMethod.GET, url).build();
    HttpResponse response = remoteClient.execute(tokenRequest, Idempotency.NONE);

    if (response.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new NotFoundException(
          String.format("Credential Identity not found for namespace '%s'.", namespace));
    }

    if (response.getResponseCode() != HttpResponseStatus.OK.code()) {
      throw new CredentialProvisioningException(String.format(
          "Failed to provision credential with response code: %s and error: %s",
          response.getResponseCode(),
          response.getResponseBodyAsString()));
    }

    return GSON.fromJson(response.getResponseBodyAsString(), ProvisionedCredential.class);
  }

  @Override
  public void validateIdentity(String namespace, String serviceAccount)
      throws IdentityValidationException, IOException {
    String url = String.format("namespaces/%s/credentials/workloadIdentity/validate",
        namespace);
    io.cdap.common.http.HttpRequest tokenRequest =
        remoteClient.requestBuilder(HttpMethod.POST, url)
            .withBody(GSON.toJson(new NamespaceWorkloadIdentity(serviceAccount))).build();
    HttpResponse response = remoteClient.execute(tokenRequest, Idempotency.NONE);

    if (response.getResponseCode() != HttpResponseStatus.OK.code()) {
      throw new IdentityValidationException(String.format(
          "Failed to provision credential with response code: %s and error: %s",
          response.getResponseCode(),
          response.getResponseBodyAsString()));
    }
  }
}
