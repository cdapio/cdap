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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.credential.CredentialProvisionContext;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.credential.SecurityTokenServiceRequest.TokenType;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.ApiResponse;
import io.kubernetes.client.openapi.models.AuthenticationV1TokenRequest;
import io.kubernetes.client.openapi.models.V1TokenRequestStatus;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit Tests for {@link GcpWorkloadIdentityCredentialProvider}.
 */
public class GcpWorkloadIdentityCredentialProviderTest {

  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
      new BasicThrowableCodec()).create();
  private static final String IAM_TOKEN = "iam-token";
  private static final String EXPIRES_IN = LocalDateTime.now().format(
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));

  @Test
  public void testProvisioningCredentialWithRetries() throws Exception {

    GcpWorkloadIdentityCredentialProvider gcpWorkloadIdentityCredentialProvider =
        new GcpWorkloadIdentityCredentialProvider();

    CredentialProviderContext credentialProviderContext = getCredentialProviderContext();
    gcpWorkloadIdentityCredentialProvider.initialize(credentialProviderContext);

    GcpWorkloadIdentityCredentialProvider mockedCredentialProvider =
        spy(gcpWorkloadIdentityCredentialProvider);

    LoadingCache<ProvisionedCredentialCacheKey,
        ProvisionedCredential> cache = CacheBuilder.newBuilder()
        .build(new CacheLoader<ProvisionedCredentialCacheKey, ProvisionedCredential>() {
          @Override
          public ProvisionedCredential load(ProvisionedCredentialCacheKey
              provisionedCredentialCacheKey) throws Exception {
            return mockedCredentialProvider.getProvisionedCredential(
                provisionedCredentialCacheKey.getK8sNamespace(),
                provisionedCredentialCacheKey.getCredentialIdentity(),
                provisionedCredentialCacheKey.getScopes());
          }
        });

    doReturn(cache).when(mockedCredentialProvider).getCredentialLoadingCache();

    doThrow(new SocketTimeoutException())
        .doThrow(new ConnectException())
        .doReturn(getSecurityTokenServiceResponse())
        .doReturn(getIamCredentialGenerateAccessTokenResponse())
        .when(mockedCredentialProvider).executeHttpPostRequest(any(), anyString(), any());

    doReturn(getMockApiClient()).when(mockedCredentialProvider).getApiClient();

    CredentialProfile credentialProfile = new CredentialProfile(
        GcpWorkloadIdentityCredentialProvider.NAME, "profile", Collections.emptyMap());
    Map<String, String> properties = new HashMap<>();
    properties.put(GcpWorkloadIdentityCredentialProvider.K8S_NAMESPACE_PROPERTY, "default");
    properties
        .put(GcpWorkloadIdentityCredentialProvider.GCP_WRAPPED_CDAP_NAMESPACE_PROPERTY, "default");

    // validate profile
    mockedCredentialProvider.validateProfile(credentialProfile);

    CredentialIdentity credentialIdentity = new CredentialIdentity(
        NamespaceId.DEFAULT.getNamespace(), "default",
        NamespaceMeta.DEFAULT.getIdentity(), "secureVal");
    // provision credential
    ProvisionedCredential credential =
        mockedCredentialProvider.provision(NamespaceId.SYSTEM.getNamespace(), credentialProfile,
            credentialIdentity, new CredentialProvisionContext(properties));

    Assert.assertEquals(credential.get(), IAM_TOKEN);
    Assert.assertEquals(credential.getExpiration().toString(), EXPIRES_IN);
    // twice per invocation of
    // {@link GcpWorkloadIdentityCredentialProvider#getProvisionedCredential}
    verify(mockedCredentialProvider, times(4)).getApiClient();
  }

  private String getSecurityTokenServiceResponse() {
    SecurityTokenServiceResponse securityTokenServiceResponse =
        new SecurityTokenServiceResponse("token", TokenType.ACCESS_TOKEN,
            "Bearer", 3600);
    return GSON.toJson(securityTokenServiceResponse);
  }

  private String getIamCredentialGenerateAccessTokenResponse() {
    IamCredentialGenerateAccessTokenResponse iamCredentialGenerateAccessTokenResponse =
        new IamCredentialGenerateAccessTokenResponse(IAM_TOKEN, EXPIRES_IN);
    return GSON.toJson(iamCredentialGenerateAccessTokenResponse);
  }

  private ApiClient getMockApiClient() throws ApiException {
    ApiClient mockApiClient = mock(ApiClient.class);
    when(mockApiClient.escapeString(anyString())).thenCallRealMethod();
    ApiResponse<Object> apiResponse = getAuthenticationTokenRequestResponse();
    doThrow(new ApiException(500, "Service is unavailable"))
        .doReturn(apiResponse)
        .when(mockApiClient).execute(any(), any());
    return mockApiClient;
  }

  private V1TokenRequestStatus getV1TokenRequestStatus() {
    V1TokenRequestStatus v1TokenRequestStatus = new V1TokenRequestStatus();
    v1TokenRequestStatus.setToken("auth-token");
    v1TokenRequestStatus.setExpirationTimestamp(OffsetDateTime.now().plusHours(1L));
    return v1TokenRequestStatus;
  }

  private ApiResponse<Object> getAuthenticationTokenRequestResponse() {
    AuthenticationV1TokenRequest authenticationV1TokenRequest =
        mock(AuthenticationV1TokenRequest.class);
    doReturn(getV1TokenRequestStatus()).when(authenticationV1TokenRequest).getStatus();
    return new
        ApiResponse<Object>(200, Collections.emptyMap(), authenticationV1TokenRequest);
  }

  private CredentialProviderContext getCredentialProviderContext() {
    Map<String, String> properties = new HashMap<>();
    properties.put(GcpWorkloadIdentityCredentialProvider.WORKLOAD_IDENTITY_PROVIDER, "provider");
    properties.put(GcpWorkloadIdentityCredentialProvider.WORKLOAD_IDENTITY_POOL, "pool");
    properties.put(GcpWorkloadIdentityCredentialProvider.RETRY_BASE_DELAY_MILLIS, "0");
    properties.put(GcpWorkloadIdentityCredentialProvider.RETRY_MAX_DELAY_MILLIS, "1");
    properties.put(GcpWorkloadIdentityCredentialProvider.RETRY_TIMEOUT_SECS, "10");
    return new CredentialProviderContext() {
      @Override
      public Map<String, String> getProperties() {
        return properties;
      }

      @Override
      public boolean isNamespaceCreationHookEnabled() {
        return false;
      }
    };
  }

  @Test(expected = ProfileValidationException.class)
  public void testInvalidProfile() throws ProfileValidationException {
    GcpWorkloadIdentityCredentialProvider gcpWorkloadIdentityCredentialProvider =
        new GcpWorkloadIdentityCredentialProvider();
    CredentialProfile invalidCredentialProfile = new CredentialProfile(
        "unknown-provider", "profile", Collections.emptyMap());
    gcpWorkloadIdentityCredentialProvider.validateProfile(invalidCredentialProfile);
  }

  @Test(expected = IOException.class)
  public void testExecuteHttpPostRequestHandlesHttpErrorResponse() throws Exception {
    InputStream errorMessageStream = new ByteArrayInputStream(
        "Some error here".getBytes(StandardCharsets.UTF_8));
    OutputStream outputStream = new ByteArrayOutputStream();
    HttpURLConnection mockConnection = mock(HttpURLConnection.class);
    when(mockConnection.getResponseCode()).thenReturn(500);
    when(mockConnection.getOutputStream()).thenReturn(outputStream);
    when(mockConnection.getInputStream()).thenThrow(new AssertionError("wrong exception"));
    when(mockConnection.getErrorStream()).thenReturn(errorMessageStream);
    GcpWorkloadIdentityCredentialProvider gcpWorkloadIdentityCredentialProvider =
        new GcpWorkloadIdentityCredentialProvider();
    gcpWorkloadIdentityCredentialProvider
        .executeHttpPostRequest(() -> mockConnection, "some body", new HashMap<>());
  }
}
