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
 *
 */

package io.cdap.cdap.datapipeline.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.security.store.lease.SecureStoreLease;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.datapipeline.oauth.CredentialIsValidResponse;
import io.cdap.cdap.datapipeline.oauth.GetAccessTokenResponse;
import io.cdap.cdap.datapipeline.oauth.OAuthAccessToken;
import io.cdap.cdap.datapipeline.oauth.OAuthClientCredentials;
import io.cdap.cdap.datapipeline.oauth.OAuthProvider;
import io.cdap.cdap.datapipeline.oauth.OAuthProvider.CredentialEncodingStrategy;
import io.cdap.cdap.datapipeline.oauth.OAuthRefreshToken;
import io.cdap.cdap.datapipeline.oauth.OAuthStore;
import io.cdap.cdap.datapipeline.oauth.OAuthStoreException;
import io.cdap.cdap.datapipeline.oauth.PutOAuthCredentialRequest;
import io.cdap.cdap.datapipeline.oauth.PutOAuthProviderRequest;
import io.cdap.cdap.datapipeline.oauth.RefreshTokenResponse;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth handler.
 */
public class OAuthHandler extends AbstractSystemHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(OAuthHandler.class);
  private static final String API_VERSION = "v1";
  private static final Gson GSON = new GsonBuilder()
    .setPrettyPrinting()
    .registerTypeAdapterFactory(new ErrorHandlingGsonTypeAdapterFactory())
    .create();

  private static final String WORKER_ID_PREFIX = generateWorkerIdPrefix();

  // The following settings can be overridden via CDAP System Preferences
  // Margin of safety before an access token officially expires to preemptively refresh it
  private static final String PREF_ACCESS_TOKEN_SAFETY_BUFFER_MS = "oauth.rtr.access.token.safety.buffer.ms";
  // Maximum time to block and wait for another instance to complete a token refresh
  private static final String PREF_WAIT_FOR_TOKEN_TIMEOUT_MS = "oauth.rtr.wait.timeout.ms";
  // Interval at which to poll the token store while waiting for a concurrent refresh
  private static final String PREF_WAIT_FOR_TOKEN_POLL_INTERVAL_MS = "oauth.rtr.wait.poll.interval.ms";
  // The duration to hold the distributed refresh lock before it auto-expires
  private static final String PREF_LEASE_EXPIRATION_TIMEOUT_MS = "oauth.rtr.lease.expiration.timeout.ms";

  private long accessTokenSafetyBufferMs;
  private long waitForTokenTimeoutMs;
  private long waitForTokenPollIntervalMs;
  private long leaseExpirationTimeoutMs;

  private OAuthStore oauthStore;

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    this.oauthStore = new OAuthStore(context, context, context.getAdmin());
    
    Map<String, String> prefs = null;
    try {
      prefs = context.getPreferencesForNamespace(NamespaceId.SYSTEM.getNamespace(), true);
    } catch (Exception e) {
      LOG.warn("Failed to load preferences for OAuth RTR configuration. Using default values.", e);
    }

    accessTokenSafetyBufferMs = getPrefOrDefault(prefs, PREF_ACCESS_TOKEN_SAFETY_BUFFER_MS, 600_000L);
    waitForTokenTimeoutMs = getPrefOrDefault(prefs, PREF_WAIT_FOR_TOKEN_TIMEOUT_MS, 100_000L);
    waitForTokenPollIntervalMs = getPrefOrDefault(prefs, PREF_WAIT_FOR_TOKEN_POLL_INTERVAL_MS, 500L);
    leaseExpirationTimeoutMs = getPrefOrDefault(prefs, PREF_LEASE_EXPIRATION_TIMEOUT_MS, 90_000L);
  }

  @GET
  @Path(API_VERSION + "/oauth/provider/{provider}/authurl")
  public void getAuthURL(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("provider") String provider,
                         @QueryParam("redirect_uri") String redirectURI,
                         @QueryParam("redirect_url") String redirectURL) {
    try {
      OAuthProvider oauthProvider = getProvider(provider);

      String formatURL = "%s";
      String loginUrl = oauthProvider.getLoginURL();
      if (!loginUrl.contains("?")) {
        formatURL += "?";
      } else if (!loginUrl.endsWith("&")) {
        formatURL += "&";
      }
      formatURL += "client_id=%s&redirect_uri=%s";

      // Maintaining backward compatibility for the apps using "redirect_url" parameter.
      if (redirectURI == null || redirectURI.isEmpty()) {
        redirectURI = redirectURL;
      }

      String response = String.format(
          formatURL, loginUrl, oauthProvider.getClientCredentials().getClientId(), redirectURI);
      responder.sendString(response);
    } catch (OAuthServiceException e) {
      e.respond(responder);
    }
  }

  @PUT
  @Path(API_VERSION + "/oauth/provider/{provider}")
  public void putOAuthProvider(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("provider") String oauthProvider,
                               @QueryParam("reuse_client_credentials") @DefaultValue("false")
                               Boolean reuseClientCredentials) {
    try {
      try {
        PutOAuthProviderRequest putOAuthProviderRequest = GSON.fromJson(
            StandardCharsets.UTF_8.decode(request.getContent()).toString(),
            PutOAuthProviderRequest.class);
        CredentialEncodingStrategy strategy = putOAuthProviderRequest.getCredentialEncodingStrategy();
        String userAgent = putOAuthProviderRequest.getUserAgent();
        // Validate URLs
        URL loginURL = new URL(putOAuthProviderRequest.getLoginURL());
        URL tokenRefreshURL = new URL(putOAuthProviderRequest.getTokenRefreshURL());

        LOG.info("Received putOAuthProvider request with write_client_credentials = {}", reuseClientCredentials);
        OAuthClientCredentials clientCredentials = null;
        if (!reuseClientCredentials) {
          clientCredentials = OAuthClientCredentials.newBuilder()
                                                    .withClientId(putOAuthProviderRequest.getClientId())
                                                    .withClientSecret(putOAuthProviderRequest.getClientSecret())
                                                    .build();
        }
        OAuthProvider provider = OAuthProvider.newBuilder()
                                              .withName(oauthProvider)
                                              .withLoginURL(loginURL.toString())
                                              .withTokenRefreshURL(tokenRefreshURL.toString())
                                              .withClientCredentials(clientCredentials)
                                              .withCredentialEncodingStrategy(strategy)
                                              .withUserAgent(userAgent)
                                              .withAuthType(putOAuthProviderRequest.getAuthType())
                                              .withRefreshType(putOAuthProviderRequest.getRefreshType())
                                              .build();

        if (provider.getRefreshType() == OAuthProvider.RefreshType.RTR) {
          if (!oauthStore.isLeaseSupported()) {
            throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                "Refresh Token Rotation (RTR) is only supported when GCP Secret Manager is configured.");
          }
        }

        oauthStore.writeProvider(provider, reuseClientCredentials);
        responder.sendStatus(HttpURLConnection.HTTP_OK);
      } catch (JsonSyntaxException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid JSON: " + e.getMessage(), e);
      } catch (NullPointerException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid provider: " + e.getMessage(), e);
      } catch (MalformedURLException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid URL: " + e.getMessage(), e);
      } catch (OAuthStoreException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Failed to write to OAuth store", e);
      }
    } catch (OAuthServiceException e) {
      e.respond(responder);
    }
  }

  @DELETE
  @Path(API_VERSION + "/oauth/provider/{provider}")
  public void deleteOAuthProvider(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("provider") String oauthProvider,
                               @QueryParam("preserve_client_credentials") @DefaultValue("false")
                               boolean preserveClientCredentials) {
    try {
      try {
        oauthStore.deleteProvider(oauthProvider, preserveClientCredentials);
        responder.sendStatus(HttpURLConnection.HTTP_OK);
      } catch (NullPointerException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid provider: " + e.getMessage(), e);
      } catch (OAuthStoreException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Failed to delete OAuth provider.", e);
      }
    } catch (OAuthServiceException e) {
      e.respond(responder);
    }
  }

  @PUT
  @Path(API_VERSION + "/oauth/provider/{provider}/credential/{credential}")
  public void putOAuthCredential(HttpServiceRequest request, HttpServiceResponder responder,
                                 @PathParam("provider") String provider,
                                 @PathParam("credential") String credentialId) {
    try {
      PutOAuthCredentialRequest putOAuthCredentialRequest;
      try {
        putOAuthCredentialRequest = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(),
                PutOAuthCredentialRequest.class);
        if (putOAuthCredentialRequest.getOneTimeCode() == null
            || putOAuthCredentialRequest.getOneTimeCode().isEmpty()) {
          throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid request: missing one-time code");
        }
        if (putOAuthCredentialRequest.getRedirectURI() == null
            || putOAuthCredentialRequest.getRedirectURI().isEmpty()) {
          throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid request: missing redirect URI");
        }
      } catch (JsonSyntaxException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid JSON: " + e.getMessage(), e);
      }

      OAuthProvider oauthProvider = getProvider(provider);

      HttpResponse response;
      try {
        response = HttpRequests.execute(createGetRefreshTokenRequest(
            oauthProvider,
            putOAuthCredentialRequest.getOneTimeCode(),
            putOAuthCredentialRequest.getRedirectURI()));
      } catch (IOException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Error while fetching refresh token", e);
      }

      if (response.getResponseCode() != 200) {
        throw new OAuthServiceException(
          response.getResponseCode(),
            "Request for refresh token did not return 200. Response code: "
                + response.getResponseCode()
                + " , response message: "
                + response.getResponseMessage()
                + " , response body: "
                + response.getResponseBodyAsString());
      }

      RefreshTokenResponse refreshTokenResponse;
      try {
        refreshTokenResponse = GSON.fromJson(response.getResponseBodyAsString(), RefreshTokenResponse.class);
      } catch (JsonSyntaxException e) {
        throw new OAuthServiceException(
            HttpURLConnection.HTTP_INTERNAL_ERROR, "Failed to parse JSON: " + e.getMessage(), e);
      }

      boolean hasRefreshToken = refreshTokenResponse.getRefreshToken() != null
          && !refreshTokenResponse.getRefreshToken().isEmpty();
      boolean hasAccessToken = refreshTokenResponse.getAccessToken() != null
          && !refreshTokenResponse.getAccessToken().isEmpty();

      if (!hasAccessToken && !hasRefreshToken) {
        throw new OAuthServiceException(
            HttpURLConnection.HTTP_BAD_REQUEST,
            String.format(
                "Refresh token response is missing the required access token or refresh token. " +
                    "The actual response received: %s",
                response.getResponseBodyAsString()));
      }

      if (hasRefreshToken) {
        try {
          OAuthRefreshToken refreshToken = OAuthRefreshToken.newBuilder()
              .withRefreshToken(refreshTokenResponse.getRefreshToken())
              .withRedirectURI(putOAuthCredentialRequest.getRedirectURI())
              .build();
          oauthStore.writeRefreshToken(provider, credentialId, refreshToken);

          // For RTR, also store the initial Access Token in OAuthStore
          if (OAuthProvider.RefreshType.RTR.equals(oauthProvider.getRefreshType())) {
            writeAccessTokenFromResponse(provider, credentialId, refreshTokenResponse);
          }
        } catch (NullPointerException e) {
          throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage(), e);
        } catch (OAuthStoreException e) {
          throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, "Failed to write credentials", e);
        }
      } else {
        // Refresh token call gave us an access token without a refresh token.
        // Store the access token instead.

        try {
          OAuthAccessToken accessToken = OAuthAccessToken.newBuilder()
              .withAccessToken(refreshTokenResponse.getAccessToken())
              .build();
          oauthStore.writeAccessToken(provider, credentialId, accessToken);
        } catch (NullPointerException e) {
          throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage(), e);
        } catch (OAuthStoreException e) {
          throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, "Failed to write access token", e);
        }
      }

      responder.sendStatus(HttpURLConnection.HTTP_OK);
    } catch (OAuthServiceException e) {
      e.respond(responder);
    }
  }

  /**
   * If a refresh token is stored, use it to request a short-lived access token from the 3rd-party OAuth API.
   * If a long-lived access token is stored, return it.
   * @param request
   * @param responder
   * @param provider ID of OAuth provider
   * @param credentialId ID of stored credential
   */
  @GET
  @Path(API_VERSION + "/oauth/provider/{provider}/credential/{credential}")
  public void getOAuthCredential(HttpServiceRequest request, HttpServiceResponder responder,
                                 @PathParam("provider") String provider,
                                 @PathParam("credential") String credentialId) {
    try {
      OAuthProvider oauthProvider = getProvider(provider);

      if (OAuthProvider.RefreshType.RTR.equals(oauthProvider.getRefreshType())) {
        getOAuthCredentialWithRefreshTokenRotation(responder, oauthProvider, provider, credentialId);
      } else {
        getOAuthCredentialStandard(responder, oauthProvider, provider, credentialId);
      }
    } catch (OAuthServiceException e) {
      e.respond(responder);
    }
  }

  private void getOAuthCredentialStandard(HttpServiceResponder responder,
                                           OAuthProvider oauthProvider,
                                           String provider,
                                           String credentialId) throws OAuthServiceException {
    // 1. Check if long-lived access token is stored (for permanent token providers)
    Optional<OAuthAccessToken> oAuthAccessToken = getAccessToken(provider, credentialId);
    if (oAuthAccessToken.isPresent()) {
      responder.sendString(GSON.toJson(
        new GetAccessTokenResponse(oAuthAccessToken.get().getAccessToken(), "")));
      return;
    }

    // 2. Fetch refresh token from store
    OAuthRefreshToken refreshToken = getRefreshToken(provider, credentialId);

    // 3. Request short-lived access token from 3rd-party API
    HttpResponse response;
    try {
      response = HttpRequests.execute(createGetAccessTokenRequest(oauthProvider, refreshToken.getRefreshToken()));
    } catch (IOException e) {
      throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Failed to fetch refresh token", e);
    }

    if (response.getResponseCode() != 200) {
      throw new OAuthServiceException(
          response.getResponseCode(),
          "Request for refresh token did not return 200. Response code: "
              + response.getResponseCode()
              + " , response message: "
              + response.getResponseMessage()
              + " , response body: "
              + response.getResponseBodyAsString());
    }

    RefreshTokenResponse refreshTokenResponse;
    try {
      refreshTokenResponse = GSON.fromJson(response.getResponseBodyAsString(), RefreshTokenResponse.class);
    } catch (JsonSyntaxException e) {
      throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Error parsing JSON response", e);
    }

    if (refreshTokenResponse.getAccessToken() == null || refreshTokenResponse.getAccessToken().isEmpty()) {
      throw new OAuthServiceException(
          HttpURLConnection.HTTP_BAD_REQUEST,
          "Access token response body does not have access token: " + response.getResponseBodyAsString());
    }

    // Standard flow: Return access token to caller without writing back to store
    responder.sendString(GSON.toJson(
        new GetAccessTokenResponse(refreshTokenResponse.getAccessToken(), refreshTokenResponse.getInstanceURL())));
  }

  private boolean isAccessTokenValid(OAuthAccessToken token) {
    // Rule 1: If expiresAt is present (> 0), use it with 10-minute safety buffer
    if (token.getExpiresAt() > 0) {
      return !token.isExpired(accessTokenSafetyBufferMs);
    }

    // Rule 2: Else if id / identityUrl is present, validate against identity URL
    if (token.getIdentityUrl() != null && !token.getIdentityUrl().isEmpty()) {
      return isAccessTokenValidViaIdentityUrl(token.getAccessToken(), token.getIdentityUrl());
    }

    // Rule 3: Otherwise -> refresh
    return false;
  }

  private boolean isAccessTokenValidViaIdentityUrl(String accessToken, String identityUrl) {
    try {
      HttpRequest request = HttpRequest.get(new URL(identityUrl))
          .addHeader("Authorization", "Bearer " + accessToken)
          .build();
      HttpResponse response = HttpRequests.execute(request);
      return response.getResponseCode() == 200;
    } catch (Exception e) {
      LOG.warn("Failed to validate access token via identity URL {}: {}", identityUrl, e.getMessage());
      return false;
    }
  }

  private GetAccessTokenResponse fetchOAuthCredentialWithRefreshTokenRotation(OAuthProvider oauthProvider,
                                                                              String provider,
                                                                              String credentialId) throws OAuthServiceException {
    // 1. Check if a valid cached access token is already available
    Optional<OAuthAccessToken> oAuthAccessToken = getAccessToken(provider, credentialId);
    if (oAuthAccessToken.isPresent() && isAccessTokenValid(oAuthAccessToken.get())) {
      LOG.debug("Returning valid cached access token for provider {} credential {}", provider, credentialId);
      return new GetAccessTokenResponse(oAuthAccessToken.get().getAccessToken(), "");
    }

    // 2. Generate a unique lock holder ID for this request thread and try to acquire lease lock
    String lockHolderId = WORKER_ID_PREFIX + "-" + UUID.randomUUID();
    SecureStoreLease lease = acquireLeaseLockWithRetries(provider, credentialId, leaseExpirationTimeoutMs, lockHolderId);

    // 3. If lock is held by another process -> wait for published token
    if (!lease.isAcquired()) {
      LOG.info("Lease lock held by another process for provider {} credential {}. Waiting for new access token...",
               provider, credentialId);
      Optional<GetAccessTokenResponse> waitedResponse = waitForNewAccessToken(
          provider, credentialId, waitForTokenTimeoutMs, waitForTokenPollIntervalMs);
      if (waitedResponse.isPresent()) {
        return waitedResponse.get();
      }

      // Timeout occurred while waiting for winner -> Attempt to acquire lease lock again!
      lease = acquireLeaseLockWithRetries(provider, credentialId, leaseExpirationTimeoutMs, lockHolderId);
      if (!lease.isAcquired()) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_CLIENT_TIMEOUT,
                                        "Timed out waiting for OAuth access token refresh for " + credentialId);
      }
    }

    // 4. Winner (either initial or fallback after timeout) executes token refresh and persistence
    return refreshAndStoreTokens(oauthProvider, lease, provider, credentialId);
  }

  private void getOAuthCredentialWithRefreshTokenRotation(HttpServiceResponder responder,
                                                          OAuthProvider oauthProvider,
                                                          String provider,
                                                          String credentialId) throws OAuthServiceException {
    GetAccessTokenResponse response = fetchOAuthCredentialWithRefreshTokenRotation(oauthProvider, provider, credentialId);
    responder.sendString(GSON.toJson(response));
  }

  private SecureStoreLease acquireLeaseLock(String provider, String credentialId, long timeoutMs, String lockHolder)
          throws OAuthServiceException {
    try {
      return oauthStore.acquireLease(provider, credentialId, timeoutMs, lockHolder);
    } catch (Exception e) {
      throw new OAuthServiceException(HttpURLConnection.HTTP_NOT_FOUND,
                                      "Refresh token credential not found or unauthorized for " + credentialId, e);
    }
  }

  private void writeAccessTokenFromResponse(String provider, String credentialId,
                                            RefreshTokenResponse refreshTokenResponse)
      throws OAuthStoreException {
    if (refreshTokenResponse.getAccessToken() == null || refreshTokenResponse.getAccessToken().isEmpty()) {
      return;
    }

    long expiresInSeconds = refreshTokenResponse.getExpiresIn();
    long expiresAt = expiresInSeconds > 0
        ? System.currentTimeMillis() + (expiresInSeconds * 1000L)
        : 0L;
    String identityUrl = refreshTokenResponse.getId();

    OAuthAccessToken accessToken = OAuthAccessToken.newBuilder()
        .withAccessToken(refreshTokenResponse.getAccessToken())
        .withExpiresAt(expiresAt)
        .withIdentityUrl(identityUrl)
        .build();

    oauthStore.writeAccessToken(provider, credentialId, accessToken);
  }

  private GetAccessTokenResponse refreshAndStoreTokens(OAuthProvider oauthProvider,
                                                       SecureStoreLease lease,
                                                       String provider,
                                                       String credentialId) throws OAuthServiceException {
    try {
      OAuthRefreshToken refreshToken = getRefreshToken(provider, credentialId);
      HttpResponse response;
      try {
        response = HttpRequests.execute(createGetAccessTokenRequest(oauthProvider, refreshToken.getRefreshToken()));
      } catch (IOException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Failed to fetch refresh token", e);
      }

      if (response.getResponseCode() != 200) {
        throw new OAuthServiceException(
            response.getResponseCode(),
            "Request for refresh token did not return 200: " + response.getResponseBodyAsString());
      }

      RefreshTokenResponse refreshTokenResponse;
      try {
        refreshTokenResponse = GSON.fromJson(response.getResponseBodyAsString(), RefreshTokenResponse.class);
      } catch (JsonSyntaxException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Error parsing JSON response", e);
      }

      if (refreshTokenResponse.getAccessToken() == null || refreshTokenResponse.getAccessToken().isEmpty()) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, "Access token missing in response");
      }

      // Mandatory Writes for RTR:
      // Write Refresh Token v2 FIRST
      if (refreshTokenResponse.getRefreshToken() != null && !refreshTokenResponse.getRefreshToken().isEmpty()) {
        OAuthRefreshToken rotatedRefreshToken = OAuthRefreshToken.newBuilder()
            .withRefreshToken(refreshTokenResponse.getRefreshToken())
            .withRedirectURI(refreshToken.getRedirectURI())
            .build();
        try {
          oauthStore.writeRefreshToken(provider, credentialId, rotatedRefreshToken);
        } catch (OAuthStoreException e) {
          throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR,
                                          "Failed to write rotated refresh token", e);
        }
      }

      // Write Access Token v2 SECOND
      try {
        writeAccessTokenFromResponse(provider, credentialId, refreshTokenResponse);
      } catch (OAuthStoreException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Failed to write access token", e);
      }

      return new GetAccessTokenResponse(refreshTokenResponse.getAccessToken(), refreshTokenResponse.getInstanceURL());
    } finally {
      if (lease != null && lease.isAcquired()) {
        try {
          oauthStore.releaseLease(provider, credentialId, lease);
        } catch (Exception e) {
          LOG.warn("Failed to release lease lock for provider {} credential {}: {}",
                   provider, credentialId, e.getMessage());
        }
      }
    }
  }

  private Optional<GetAccessTokenResponse> waitForNewAccessToken(
      String provider, String credentialId, long maxWaitMs, long pollIntervalMs) {
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTime < maxWaitMs) {
      try {
        Thread.sleep(pollIntervalMs);

        // Poll OAuthStore to check if lock winner published a valid access token
        Optional<OAuthAccessToken> accessToken = getAccessToken(provider, credentialId);
        if (accessToken.isPresent() && isAccessTokenValid(accessToken.get())) {
          return Optional.of(new GetAccessTokenResponse(accessToken.get().getAccessToken(), ""));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return Optional.empty();
      } catch (Exception e) {
        LOG.debug("Waiting for new access token for provider {} credential {}: {}",
                  provider, credentialId, e.getMessage());
      }
    }

    return Optional.empty();
  }

  @GET
  @Path(API_VERSION + "/oauth/provider/{provider}/credential/{credential}/valid")
  public void getOAuthCredentialValidity(HttpServiceRequest request, HttpServiceResponder responder,
                                         @PathParam("provider") String provider,
                                         @PathParam("credential") String credentialId) {
    try {
      OAuthProvider oauthProvider = getProvider(provider);

      if (OAuthProvider.RefreshType.RTR.equals(oauthProvider.getRefreshType())) {
        try {
          fetchOAuthCredentialWithRefreshTokenRotation(oauthProvider, provider, credentialId);
          responder.sendString(GSON.toJson(new CredentialIsValidResponse(true)));
        } catch (OAuthServiceException e) {
          if (e.getStatus() == HttpURLConnection.HTTP_UNAUTHORIZED || e.getStatus() == HttpURLConnection.HTTP_BAD_REQUEST) {
            responder.sendString(GSON.toJson(new CredentialIsValidResponse(false)));
          } else {
            throw e;
          }
        }
        return;
      }

      Optional<OAuthAccessToken> oAuthAccessToken = getAccessToken(provider, credentialId);

      if (oAuthAccessToken.isPresent()) {
        responder.sendString(GSON.toJson(new CredentialIsValidResponse(true)));
        return;
      }

      OAuthRefreshToken refreshToken = getRefreshToken(provider, credentialId);

      HttpResponse response;
      try {
        response = HttpRequests.execute(createGetAccessTokenRequest(oauthProvider, refreshToken.getRefreshToken()));
      } catch (IOException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Error while fetching refresh token", e);
      }

      responder.sendString(GSON.toJson(new CredentialIsValidResponse(checkCredIsValid(response))));
    } catch (OAuthServiceException e) {
      e.respond(responder);
    }
  }

  private boolean checkCredIsValid(HttpResponse response) throws OAuthServiceException {
    if (response.getResponseCode() != 200) {
      return false;
    }

    RefreshTokenResponse refreshTokenResponse;
    try {
      refreshTokenResponse = GSON.fromJson(response.getResponseBodyAsString(), RefreshTokenResponse.class);
    } catch (JsonSyntaxException e) {
      throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Failed to parse JSON", e);
    }

    return !(refreshTokenResponse.getAccessToken() == null || refreshTokenResponse.getAccessToken().isEmpty());
  }

  /**
   * Create the request body for refresh token & access token requests
   * @param strategy which encoding strategy is used to send client ID + secret
   * @param grantType whether an authorization code used to fetch a refresh token or a refresh token used to fetch an
   *                  access token is used
   * @param code used when building a request to get a refresh token
   * @param redirectURI used when building a request to get an access token
   * @param refreshToken used when building a request to get an access token
   * @param clientCreds the client ID + secret
   * @return request body
   */
  private String buildRequestBody(CredentialEncodingStrategy strategy,
                                  String grantType,
                                  String code,
                                  String redirectURI,
                                  String refreshToken,
                                  OAuthClientCredentials clientCreds) {
    switch (strategy) {
      case BASIC_AUTH:
        return grantType.equals("authorization_code")
                ? String.format("code=%s&redirect_uri=%s&grant_type=%s", code, redirectURI, grantType)
                : String.format("grant_type=%s&refresh_token=%s", grantType, refreshToken);
      case FORM_BODY: // fall-through
      default:
        return grantType.equals("authorization_code")
                ? String.format("code=%s&redirect_uri=%s&client_id=%s&client_secret=%s&grant_type=%s",
                code, redirectURI, clientCreds.getClientId(), clientCreds.getClientSecret(), grantType)
                : String.format("grant_type=%s&client_id=%s&client_secret=%s&refresh_token=%s",
                grantType, clientCreds.getClientId(), clientCreds.getClientSecret(), refreshToken);
    }
  }

  /** Build HTTP request for getting tokens */
  private HttpRequest.Builder buildHttpRequest(String body,
                                               CredentialEncodingStrategy strategy,
                                               OAuthClientCredentials clientCreds,
                                               String refreshTokenURL,
                                               boolean addContentType,
                                               String userAgent) throws MalformedURLException {
    HttpRequest.Builder requestBuilder = HttpRequest.post(new URL(refreshTokenURL))
            .withBody(body);

    if (addContentType) {
      requestBuilder.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED);
    }

    if (strategy == CredentialEncodingStrategy.BASIC_AUTH) {
      requestBuilder.addHeader(HttpHeaders.AUTHORIZATION, getBasicAuthHeader(clientCreds));
    }

    if (userAgent != null) {
      requestBuilder.addHeader(HttpHeaders.USER_AGENT, userAgent);
    }

    return requestBuilder;
  }

  /**
   * Build the HttpRequest to request a refresh token from the OAuth provider
   * @param provider
   * @param code the authorization code given after the user accepts OAuth from the provider
   * @param redirectURI
   */
  private HttpRequest createGetRefreshTokenRequest(OAuthProvider provider, String code, String redirectURI)
      throws OAuthServiceException {
    OAuthClientCredentials clientCreds = provider.getClientCredentials();
    CredentialEncodingStrategy strategy = provider.getCredentialEncodingStrategy();
    String tokenRefreshURL = provider.getTokenRefreshURL();
    String body = buildRequestBody(strategy, "authorization_code", code, redirectURI, null, clientCreds);
    String userAgent = provider.getUserAgent();

    try {
      return buildHttpRequest(body, strategy, clientCreds, tokenRefreshURL, true, userAgent).build();
    } catch (MalformedURLException e) {
      throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Malformed URL", e);
    }
  }

  /**
   * Build the HttpRequest to request an access token for making data requests from the OAuth provider
   * @param provider
   * @param refreshToken the refresh token requested previously from the provider
   */
  private HttpRequest createGetAccessTokenRequest(OAuthProvider provider, String refreshToken)
      throws OAuthServiceException {
    OAuthClientCredentials clientCreds = provider.getClientCredentials();
    CredentialEncodingStrategy strategy = provider.getCredentialEncodingStrategy();
    String tokenRefreshURL = provider.getTokenRefreshURL();
    String body = buildRequestBody(strategy, "refresh_token", null, null, refreshToken, clientCreds);
    String userAgent = provider.getUserAgent();

    try {
      return buildHttpRequest(body, strategy, clientCreds, tokenRefreshURL, false, userAgent).build();
    } catch (MalformedURLException e) {
      throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Malformed URL", e);
    }
  }

  private String getBasicAuthHeader(OAuthClientCredentials clientCreds) {
    String authInfo = String.format("%s:%s", clientCreds.getClientId(), clientCreds.getClientSecret());
    return String.format("Basic %s", Base64.getEncoder().encodeToString(authInfo.getBytes()));
  }

  private OAuthProvider getProvider(String provider) throws OAuthServiceException {
    try {
      Optional<OAuthProvider> providerOptional = oauthStore.getProvider(provider);
      if (providerOptional.isPresent()) {
        return providerOptional.get();
      }
      throw new OAuthServiceException(HttpURLConnection.HTTP_NOT_FOUND, "Unknown OAuth provider: " + provider);
    } catch (OAuthStoreException e) {
      throw new OAuthServiceException(
          HttpURLConnection.HTTP_INTERNAL_ERROR, "Error attempting to retrieve OAuth provider", e);
    }
  }

  /**
   * Fetch a refresh token from the secure store
   * @param provider
   * @param credentialId
   * @return a long-lived refresh token stored in the secure store
   * @throws OAuthServiceException
   */
  private OAuthRefreshToken getRefreshToken(String provider, String credentialId) throws OAuthServiceException {
    try {
      Optional<OAuthRefreshToken> refreshTokenOptional = oauthStore.getRefreshToken(provider, credentialId);
      if (refreshTokenOptional.isPresent()) {
        return refreshTokenOptional.get();
      }
      throw new OAuthServiceException(HttpURLConnection.HTTP_NOT_FOUND, "Unknown OAuth credential: " + credentialId);
    } catch (OAuthStoreException e) {
      throw new OAuthServiceException(
          HttpURLConnection.HTTP_INTERNAL_ERROR, "Failed to read OAuth credential from secure store", e);
    }
  }

  /**
   * Fetch a long-lived access token from the secure store.
   * @param provider
   * @param credentialId
   * @return a long-lived access token stored in the secure store
   * @throws OAuthServiceException
   */
  private Optional<OAuthAccessToken> getAccessToken(String provider, String credentialId)
      throws OAuthServiceException {
    try {
      return oauthStore.getAccessToken(provider, credentialId);
    } catch (OAuthStoreException e) {
      throw new OAuthServiceException(
          HttpURLConnection.HTTP_INTERNAL_ERROR, "Failed to read OAuth access token from secure store", e);
    }
  }

  private static class OAuthServiceException extends Exception {
    private final int status;

    OAuthServiceException(int status, String message, Throwable cause) {
      super(message, cause);
      this.status = status;
    }

    OAuthServiceException(int status, String message) {
      super(message);
      this.status = status;
    }

    int getStatus() {
      return this.status;
    }

    void respond(HttpServiceResponder responder) {
      if (status == HttpURLConnection.HTTP_INTERNAL_ERROR) {
        LOG.error("An internal error has occurred", this);
        responder.sendError(status, "Internal error");
      } else {
        responder.sendError(status, getMessage());
      }
    }
  }

  private long getPrefOrDefault(java.util.Map<String, String> prefs, String key, long defaultValue) {
    if (prefs != null && prefs.containsKey(key)) {
      try {
        return Long.parseLong(prefs.get(key));
      } catch (NumberFormatException e) {
        LOG.warn("Invalid number format for preference {}. Using default value: {}", key, defaultValue);
      }
    }
    return defaultValue;
  }

  private SecureStoreLease acquireLeaseLockWithRetries(String provider, String credentialId, long timeoutMs, String lockHolder) 
      throws OAuthServiceException {
    SecureStoreLease lease = null;
    int maxRetries = 3;
    
    for (int i = 0; i < maxRetries; i++) {
      lease = acquireLeaseLock(provider, credentialId, timeoutMs, lockHolder);
      if (lease != null) {
        return lease;
      }
      try {
        java.util.concurrent.TimeUnit.MILLISECONDS.sleep(500); // Backoff before retry
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Interrupted while acquiring lock.");
      }
    }
    
    throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, 
        "Failed to communicate with the secure store to acquire a lock for " + credentialId);
  }

  private static String generateWorkerIdPrefix() {
    String instanceName = System.getenv("HOSTNAME");
    if (instanceName == null || instanceName.isEmpty()) {
      try {
        instanceName = java.net.InetAddress.getLocalHost().getHostName();
      } catch (java.net.UnknownHostException e) {
        instanceName = "cdf";
      }
    }
    return instanceName;
  }
}
