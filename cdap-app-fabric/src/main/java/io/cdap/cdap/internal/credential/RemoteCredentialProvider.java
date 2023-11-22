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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.retry.Idempotency;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProvider;
import io.cdap.cdap.proto.credential.CredentialProvisionContext;
import io.cdap.cdap.proto.credential.CredentialProvisioningException;
import io.cdap.cdap.proto.credential.IdentityValidationException;
import io.cdap.cdap.proto.credential.NotFoundException;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.proto.credential.ValidateIdentityRequest;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;

/**
 * Remote implementation for {@link CredentialProvider} used in {@link
 * io.cdap.cdap.common.conf.Constants.ArtifactLocalizer}.
 */
public class RemoteCredentialProvider implements CredentialProvider {

  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
      new BasicThrowableCodec()).create();
  private final RemoteClient remoteClient;

  /**
   * Construct the {@link RemoteCredentialProvider}.
   *
   * @param remoteClientFactory   A factory to create {@link RemoteClient}.
   * @param internalAuthenticator An authenticator to propagate internal identity headers.
   */
  public RemoteCredentialProvider(RemoteClientFactory remoteClientFactory,
      InternalAuthenticator internalAuthenticator) {

    this.remoteClient = remoteClientFactory.createRemoteClient(Constants.Service.APP_FABRIC_HTTP,
        RemoteClientFactory.NO_VERIFY_HTTP_REQUEST_CONFIG, Constants.Gateway.INTERNAL_API_VERSION_3,
        internalAuthenticator);
  }

  /**
   * Provisions a short-lived credential for the provided identity using the provided identity.
   *
   * @param namespace    The identity namespace.
   * @param identityName The identity name.
   * @param context      The context to use for provisioning.
   * @return A short-lived credential.
   * @throws CredentialProvisioningException If provisioning the credential fails.
   * @throws IOException                     If any transport errors occur.
   * @throws NotFoundException               If the profile or identity are not found.
   */
  @Override
  public ProvisionedCredential provision(String namespace, String identityName,
      CredentialProvisionContext context)
      throws CredentialProvisioningException, IOException, NotFoundException {
    String url = String.format("namespaces/%s/credentials/identities/%s/provision",
        namespace, identityName);
    io.cdap.common.http.HttpRequest tokenRequest =
        remoteClient.requestBuilder(HttpMethod.POST, url).withBody(GSON.toJson(context)).build();
    HttpResponse response = remoteClient.execute(tokenRequest, Idempotency.NONE);

    if (response.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new NotFoundException(String.format("Credential Identity %s Not Found.",
          identityName));
    }

    if (response.getResponseCode() != HttpResponseStatus.OK.code()) {
      throw new CredentialProvisioningException(String.format(
          "Failed to provision credential with response code: %s and error: %s",
          response.getResponseCode(),
          response.getResponseBodyAsString()));
    }

    return GSON.fromJson(response.getResponseBodyAsString(), ProvisionedCredential.class);
  }

  /**
   * Validates the provided identity.
   *
   * @param namespace The identity namespace.
   * @param identity  The identity to validate.
   * @param context   The context to use for provisioning.
   * @throws IdentityValidationException If validation fails.
   * @throws IOException                 If any transport errors occur.
   * @throws NotFoundException           If the profile is not found.
   */
  @Override
  public void validateIdentity(String namespace, CredentialIdentity identity,
      CredentialProvisionContext context)
      throws IdentityValidationException, IOException, NotFoundException {
    String url = String.format("namespaces/%s/credentials/identities/validate", namespace);
    io.cdap.common.http.HttpRequest tokenRequest =
        remoteClient.requestBuilder(HttpMethod.POST, url)
            .withBody(GSON.toJson(new ValidateIdentityRequest(identity, context))).build();
    HttpResponse response = remoteClient.execute(tokenRequest, Idempotency.NONE);
    if (response.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new NotFoundException(String.format("Credential Profile %s Not Found.",
          identity.getProfileName()));
    }
    if (response.getResponseCode() == HttpResponseStatus.BAD_REQUEST.code()) {
      throw new IdentityValidationException(response.getResponseBodyAsString());
    }
    if (response.getResponseCode() != HttpResponseStatus.OK.code()) {
      throw new IOException(String.format(
          "Identity failed validation with response code: %s and error: %s",
          response.getResponseCode(),
          response.getResponseBodyAsString()));
    }
  }
}
