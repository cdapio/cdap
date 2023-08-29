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

package io.cdap.cdap.security.spi.credential;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.credential.CredentialProvisioningException;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.credential.SecurityTokenServiceRequest.GrantType;
import io.cdap.cdap.security.spi.credential.SecurityTokenServiceRequest.TokenType;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.AuthenticationV1TokenRequest;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1TokenRequestSpec;
import io.kubernetes.client.util.Config;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link CredentialProvider} Credential Provider which returns application default credentials.
 * For more details, see
 * medium.com/google-cloud/gcp-workload-identity-federation-with-federated-tokens-d03b8bad0228
 */
public class GcpWorkloadIdentityCredentialProvider implements CredentialProvider {

  public static final String NAME = "gcp-wi-credential-provider";
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
      new BasicThrowableCodec()).create();
  private static final String TOKEN_EXCHANGE_AUDIENCE_FORMAT = "identitynamespace:%s:%s";
  private static final Logger LOG =
      LoggerFactory.getLogger(GcpWorkloadIdentityCredentialProvider.class);
  private CredentialProviderContext credentialProviderContext;
  private ApiClient client;
  static final String CONNECT_TIMEOUT_SECS = "k8s.api.client.connect.timeout.secs";
  static final String CONNECT_TIMEOUT_SECS_DEFAULT = "120";
  static final String READ_TIMEOUT_SECS = "k8s.api.client.read.timeout.secs";
  static final String READ_TIMEOUT_SECS_DEFAULT = "120";
  static final String WORKLOAD_IDENTITY_POOL = "k8s.workload.identity.pool";
  static final String WORKLOAD_IDENTITY_PROVIDER = "k8s.workload.identity.provider";
  static final String RETRY_MAX_DELAY_MILLIS = "retry.policy.max.delay.ms";
  static final String RETRY_MAX_DELAY_MILLIS_DEFAULT = "10000";
  static final String RETRY_BASE_DELAY_MILLIS = "retry.policy.base.delay.ms";
  static final String RETRY_BASE_DELAY_MILLIS_DEFAULT = "200";
  static final String RETRY_TIMEOUT_SECS = "retry.policy.max.time.secs";
  static final String RETRY_TIMEOUT_SECS_DEFAULT = "300";
  private static final double RETRY_DELAY_MULTIPLIER = 1.2d;
  private static final double RETRY_RANDOMIZE_FACTOR = 0.1d;
  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";
  private static final String PROVISIONING_FAILURE_ERROR_MESSAGE_FORMAT =
      "Failed to provision credential with identity '%s'";
  static final String NAMESPACE_CREATION_HOOK_ENABLED = "namespaces.creation.hook.enabled";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void initialize(CredentialProviderContext credentialProviderContext) {
    this.credentialProviderContext = credentialProviderContext;
    LOG.info("Initialized Gcp Workload Identity Credential Provider.");
  }

  /**
   * Returns the k8s Api Client.
   *
   * @return an ApiClient
   * @throws IOException if there was a problem creating the ApiClient
   */
  @VisibleForTesting
  public ApiClient getApiClient() throws IOException {
    if (this.client != null) {
      return this.client;
    }

    this.client = Config.defaultClient();
    int connectTimeoutSec = Integer.parseInt(
        credentialProviderContext.getProperties().getOrDefault(CONNECT_TIMEOUT_SECS,
            CONNECT_TIMEOUT_SECS_DEFAULT));
    int readTimeoutSec = Integer.parseInt(
        credentialProviderContext.getProperties().getOrDefault(READ_TIMEOUT_SECS,
            READ_TIMEOUT_SECS_DEFAULT));
    OkHttpClient httpClient = this.client.getHttpClient().newBuilder()
        .connectTimeout(connectTimeoutSec, TimeUnit.SECONDS)
        .readTimeout(readTimeoutSec, TimeUnit.SECONDS)
        .build();
    this.client.setHttpClient(httpClient);

    return this.client;
  }

  @Override
  public ProvisionedCredential provision(NamespaceMeta namespaceMeta,
      CredentialProfile profile, CredentialIdentity identity)
      throws CredentialProvisioningException {

    // Provision the credential with exponential delay on retryable failure.
    long delay = Long.parseLong(
        credentialProviderContext.getProperties().getOrDefault(RETRY_BASE_DELAY_MILLIS,
            RETRY_BASE_DELAY_MILLIS_DEFAULT));
    long maxDelay = Long.parseLong(
        credentialProviderContext.getProperties().getOrDefault(RETRY_MAX_DELAY_MILLIS,
            RETRY_MAX_DELAY_MILLIS_DEFAULT));
    long timeout = Long.parseLong(
        credentialProviderContext.getProperties().getOrDefault(RETRY_TIMEOUT_SECS,
            RETRY_TIMEOUT_SECS_DEFAULT));
    double minMultiplier = RETRY_DELAY_MULTIPLIER - RETRY_DELAY_MULTIPLIER * RETRY_RANDOMIZE_FACTOR;
    double maxMultiplier = RETRY_DELAY_MULTIPLIER + RETRY_DELAY_MULTIPLIER * RETRY_RANDOMIZE_FACTOR;
    Stopwatch stopWatch = Stopwatch.createStarted();
    try {
      while (stopWatch.elapsed(TimeUnit.SECONDS) < timeout) {
        try {
          return getProvisionedCredential(namespaceMeta, identity);
        } catch (RetryableException e) {
          TimeUnit.MILLISECONDS.sleep(delay);
          delay = (long) (delay * (minMultiplier + Math.random() * (maxMultiplier - minMultiplier
              + 1)));
          delay = Math.min(delay, maxDelay);
        } catch (Exception e) {

          LOG.error(
              String.format(PROVISIONING_FAILURE_ERROR_MESSAGE_FORMAT, identity.getIdentity()), e);

          throw new CredentialProvisioningException(
              String.format(PROVISIONING_FAILURE_ERROR_MESSAGE_FORMAT, identity.getIdentity()), e);
        }
      }
    } catch (InterruptedException e) {
      throw new CredentialProvisioningException(
          String.format(PROVISIONING_FAILURE_ERROR_MESSAGE_FORMAT, identity.getIdentity()), e);
    }

    // timed out while provisioning the credential.
    throw new CredentialProvisioningException(
        new TimeoutException(
            String.format("Timed out while provisioning the credential for identity '%s'",
                identity.getIdentity())
        ));
  }

  private ProvisionedCredential getProvisionedCredential(NamespaceMeta namespaceMeta,
      CredentialIdentity identity) throws IOException, ApiException {

    // get k8s namespace from namespace metadata if namespace creation hook is enabled.
    String k8sNamespace = NamespaceId.DEFAULT.getNamespace();
    if (credentialProviderContext.isNamespaceCreationHookEnabled()) {
      k8sNamespace = namespaceMeta.getConfig().getConfigs().get("k8s.namespace");
    }

    try {
      String workloadIdentityPool =
          credentialProviderContext.getProperties().get(WORKLOAD_IDENTITY_POOL);

      // generate k8s SA token for pod
      String k8sSaToken = getK8sServiceAccountToken(k8sNamespace, identity.getIdentity(),
          workloadIdentityPool);
      LOG.trace("Successfully generated K8SA token.");

      String workloadIdentityProvider =
          credentialProviderContext.getProperties().get(WORKLOAD_IDENTITY_PROVIDER);

      // exchange JWT token via STS for Federating Token
      String tokenExchangeAudience = String.format(TOKEN_EXCHANGE_AUDIENCE_FORMAT,
          workloadIdentityPool, workloadIdentityProvider);

      LOG.trace("Exchanging JWT token for Federating Token via STS with audience {}",
          tokenExchangeAudience);
      SecurityTokenServiceResponse securityTokenServiceResponse = GSON.fromJson(
          exchangeTokenViaSts(k8sSaToken, CLOUD_PLATFORM_SCOPE, tokenExchangeAudience),
          SecurityTokenServiceResponse.class
      );
      LOG.trace("Successfully exchanged JWT token for Federating Token via STS.");

      // get GSA token using Federating Token as credential
      IamCredentialGenerateAccessTokenResponse iamCredentialGenerateAccessTokenResponse =
          GSON.fromJson(fetchIamServiceAccountToken(securityTokenServiceResponse.getAccessToken(),
          CLOUD_PLATFORM_SCOPE, identity.getSecureValue()),
              IamCredentialGenerateAccessTokenResponse.class);
      LOG.trace("Successfully generated GSA token using Federating Token as credential.");

      return new ProvisionedCredential(iamCredentialGenerateAccessTokenResponse.getAccessToken(),
          Instant.parse(iamCredentialGenerateAccessTokenResponse.getExpireTime()));

    } catch (ApiException e) {
      if ((e.getCode() / 100) != 4) {
        // if there was an API exception that was not a 4xx, we can just retry
        throw new RetryableException(e);
      }
      LOG.error("Failed to create KSA token with response code: {} and message: {}",
          e.getCode(), e.getMessage());
      throw e;
    } catch (SocketTimeoutException | ConnectException e) {
      // if there was a socket timeout or connect exception, we can just retry
      throw new RetryableException(e);
    }
  }

  @Override
  public void validateProfile(CredentialProfile profile) throws ProfileValidationException {
    if (!profile.getCredentialProviderType().equals(NAME)) {
      throw new ProfileValidationException(
          String.format("Profile is not supported by %s credential provider", NAME));
    }
  }

  private String getK8sServiceAccountToken(String namespace,
      String serviceAccountName, String audience) throws ApiException, IOException {

    // Create the TokenRequestSpec with the specified audience.
    V1TokenRequestSpec v1TokenRequestSpec = new V1TokenRequestSpec()
        .audiences(Collections.singletonList(audience))
        .expirationSeconds(3600L);

    AuthenticationV1TokenRequest authenticationV1TokenRequest = new AuthenticationV1TokenRequest()
        .apiVersion("authentication.k8s.io/v1").kind("TokenRequest").spec(v1TokenRequestSpec);

    V1ObjectMeta serviceAccountMetadata = new V1ObjectMeta()
        .name(serviceAccountName).namespace(namespace);
    authenticationV1TokenRequest.setMetadata(serviceAccountMetadata);

    CoreV1Api coreV1Api = new CoreV1Api(getApiClient());
    authenticationV1TokenRequest = coreV1Api.createNamespacedServiceAccountToken(serviceAccountName,
        namespace, authenticationV1TokenRequest, null, null,
        null, null);

    return authenticationV1TokenRequest.getStatus().getToken();
  }

  private String exchangeTokenViaSts(String token, String scopes, String audience)
      throws IOException {

    SecurityTokenServiceRequest securityTokenServiceRequest =
        new SecurityTokenServiceRequest(GrantType.TOKEN_EXCHANCE, audience, scopes,
            TokenType.ACCESS_TOKEN, TokenType.JWT, token);
    String securityTokenServiceRequestJson = GSON.toJson(securityTokenServiceRequest);
    URL url = new URL(SecurityTokenServiceRequest.STS_ENDPOINT);

    Map<String, String> headers = new HashMap<>();
    headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
    return executeHttpPostRequest(url, securityTokenServiceRequestJson, headers);
  }

  /**
   * Executes a http post request with the specified parameters.
   */
  @VisibleForTesting
  String executeHttpPostRequest(URL url, String body, Map<String, String> headers)
      throws IOException {

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod(HttpMethod.POST);
    connection.setUseCaches(false);
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      connection.setRequestProperty(entry.getKey(), entry.getValue());
    }
    connection.setDoOutput(true);

    // Write the request body to the output stream
    try (DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream())) {
      outputStream.writeBytes(body);
      outputStream.flush();
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader in = new BufferedReader(
        new InputStreamReader(connection.getInputStream()))) {
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
    }
    return response.toString();
  }

  private String fetchIamServiceAccountToken(String token, String scopes,
      String serviceAccountEmail) throws IOException {

    URL url = new URL(String.format(
        IamCredentialsGenerateAccessTokenRequest.IAM_CREDENTIALS_GENERATE_SA_TOKEN_URL_FORMAT,
        serviceAccountEmail));
    IamCredentialsGenerateAccessTokenRequest credentialsGenerateAccessTokenRequest = new
        IamCredentialsGenerateAccessTokenRequest(scopes);

    Map<String, String> headers = new HashMap<>();
    headers.put(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", token));
    headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
    String generateAccessTokenRequestJson = GSON.toJson(credentialsGenerateAccessTokenRequest);
    return executeHttpPostRequest(url, generateAccessTokenRequestJson, headers);
  }
}
