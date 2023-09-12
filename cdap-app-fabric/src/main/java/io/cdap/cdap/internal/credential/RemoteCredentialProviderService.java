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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.retry.Idempotency;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProvisioningException;
import io.cdap.cdap.proto.credential.IdentityValidationException;
import io.cdap.cdap.proto.credential.NotFoundException;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import joptsimple.internal.Strings;

/**
 * Remote implementation for {@link CredentialProviderService} used in
 * {@link io.cdap.cdap.common.conf.Constants.ArtifactLocalizer}.
 */
public class RemoteCredentialProviderService extends AbstractIdleService
    implements CredentialProviderService {
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
      new BasicThrowableCodec()).create();
  private final RemoteClient remoteClient;

  /**
   * Construct the {@link RemoteCredentialProviderService}.
   *
   * @param remoteClientFactory A factory to create {@link RemoteClient}.
   * @param internalAuthenticator An authenticator to propagate internal identity headers.
   */
  public RemoteCredentialProviderService(RemoteClientFactory remoteClientFactory,
      InternalAuthenticator internalAuthenticator) {

    this.remoteClient = remoteClientFactory.createRemoteClient(Constants.Service.APP_FABRIC_HTTP,
        RemoteClientFactory.NO_VERIFY_HTTP_REQUEST_CONFIG, Constants.Gateway.INTERNAL_API_VERSION_3,
        internalAuthenticator);
  }

  /**
   * Start the service.
   */
  @Override
  protected void startUp() throws Exception {

  }

  /**
   * Stop the service.
   */
  @Override
  protected void shutDown() throws Exception {

  }

  /**
   * Provisions a short-lived credential for the provided identity using the provided identity.
   *
   * @param namespace The identity namespace.
   * @param identityName The identity name.
   * @param scopes A comma separated list of OAuth scopes requested.
   * @return A short-lived credential.
   * @throws CredentialProvisioningException If provisioning the credential fails.
   * @throws IOException If any transport errors occur.
   * @throws NotFoundException If the profile or identity are not found.
   */
  @Override
  public ProvisionedCredential provision(String namespace, String identityName,
      String scopes) throws CredentialProvisioningException, IOException, NotFoundException {
    String url = String.format("namespaces/%s/credentials/identities/%s/provision",
        namespace, identityName);
    if (!Strings.isNullOrEmpty(scopes)) {
      url = String.format("%s?scopes=%s", url, scopes);
    }
    io.cdap.common.http.HttpRequest tokenRequest =
        remoteClient.requestBuilder(HttpMethod.GET, url).build();
    HttpResponse response = remoteClient.execute(tokenRequest, Idempotency.NONE);

    if (response.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new NotFoundException(String.format("Credential Identity %s Not Found.",
          identityName));
    }
    return GSON.fromJson(response.getResponseBodyAsString(), ProvisionedCredential.class);
  }

  /**
   * Validates the provided identity.
   *
   * @param namespaceMeta The identity namespace metadata.
   * @param identity The identity to validate.
   * @throws IdentityValidationException If validation fails.
   * @throws IOException If any transport errors occur.
   * @throws NotFoundException If the profile is not found.
   */
  @Override
  public void validateIdentity(NamespaceMeta namespaceMeta, CredentialIdentity identity)
      throws IdentityValidationException, IOException, NotFoundException {
    String url = String.format("namespaces/%s/credentials/identities/validate",
        namespaceMeta.getNamespaceId().getNamespace());
    io.cdap.common.http.HttpRequest tokenRequest =
        remoteClient.requestBuilder(HttpMethod.POST, url)
            .withBody(GSON.toJson(identity)).build();
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
