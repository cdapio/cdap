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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.api.security.store.SecureStoreInfo;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.datapipeline.oauth.AuthType;
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
import io.cdap.cdap.datapipeline.oauth.RefreshType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.common.ContentProvider;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
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
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();


  // The following OAuth settings can be configured via the system configuration(Cconf)
  // All properties related to OAuth should be prefixed with this value.
  private static final String OAUTH_CONF_PREFIX = "security.auth.oauth.";
  // Time To Live in seconds for PCKE based code verifier in Secure Store.
  public static final String PREF_PKCE_CODE_VERIFIER_TTL = "pkce.code.verifier.ttl.sec";
  // Whether to allow loopback or private network OAuth URLs. False by default in production.
  private static final String ALLOW_PRIVATE_URLS = "allow.private.urls";
  // Margin of safety before an access token officially expires to preemptively refresh it
  private static final String RTR_ACCESS_TOKEN_REFRESH_BUFFER_MS = "rtr.access.token.refresh.buffer.ms";
  // Maximum time to block and wait for another instance to complete a token refresh
  private static final String RTR_REFRESH_WAIT_TIMEOUT_MS = "rtr.refresh.wait.timeout.ms";
  // Interval at which to poll the token store while waiting for a concurrent refresh
  private static final String RTR_REFRESH_POLL_INTERVAL_MS = "rtr.refresh.poll.interval.ms";
  // The duration to hold the distributed refresh lease before it auto-expires
  private static final String RTR_LEASE_TTL_MS = "rtr.lease.ttl.ms";

  private static final String ALLOW_PRIVATE_URLS_TEST_PROP = "cdap.oauth.allow.private.urls.test.override";

  private long accessTokenRefreshBufferMs;
  private long leaseTakeoverTimeoutMs;
  private long accessTokenPollIntervalMs;
  private long leaseExpirationTimeoutMs;
  private boolean allowPrivateUrls;

  private OAuthStore oauthStore;
  private ContextAccessEnforcer contextAccessEnforcer;

  /**
   * Overrides the {@link #ALLOW_PRIVATE_URLS} configuration setting for unit tests.
   *
   * @param allow {@code true} to allow private/loopback URLs, {@code false} to disallow, or {@code null} to clear
   */
  @VisibleForTesting
  public static void setAllowPrivateUrlsForTest(@Nullable Boolean allow) {
    if (allow == null) {
      System.clearProperty(ALLOW_PRIVATE_URLS_TEST_PROP);
    } else {
      System.setProperty(ALLOW_PRIVATE_URLS_TEST_PROP, allow.toString());
    }
  }

  private boolean isAllowPrivateUrls() {
    String sysProp = System.getProperty(ALLOW_PRIVATE_URLS_TEST_PROP);
    if (sysProp != null) {
      return Boolean.parseBoolean(sysProp);
    }
    return allowPrivateUrls;
  }

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    this.contextAccessEnforcer = context.getContextAccessEnforcer();
    Map<String, String> oauthConf = context.getConfiguration(OAUTH_CONF_PREFIX);
    this.oauthStore = new OAuthStore(context, context, context.getAdmin(), oauthConf);

    accessTokenRefreshBufferMs = Long.parseLong(oauthConf.get(RTR_ACCESS_TOKEN_REFRESH_BUFFER_MS));
    leaseTakeoverTimeoutMs = Long.parseLong(oauthConf.get(RTR_REFRESH_WAIT_TIMEOUT_MS));
    accessTokenPollIntervalMs = Long.parseLong(oauthConf.get(RTR_REFRESH_POLL_INTERVAL_MS));
    leaseExpirationTimeoutMs = Long.parseLong(oauthConf.get(RTR_LEASE_TTL_MS));
    allowPrivateUrls = Boolean.parseBoolean(oauthConf.getOrDefault(ALLOW_PRIVATE_URLS, "false"));
  }

  @GET
  @Path(API_VERSION + "/oauth/provider/{provider}/authurl")
  public void getAuthURL(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("provider") String provider,
                         @QueryParam("redirect_uri") String redirectURI,
                         @QueryParam("redirect_url") String redirectURL) {
    respond(responder, () -> {
      contextAccessEnforcer.enforce(
          NamespaceId.SYSTEM.secureKey(OAuthStore.getClientCredsKey(provider)),
          StandardPermission.GET);
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
      String effectiveRedirectURI = Strings.isNullOrEmpty(redirectURI) ? redirectURL : redirectURI;

      String response = String.format(
          formatURL, loginUrl, oauthProvider.getClientCredentials().getClientId(), effectiveRedirectURI);

      if (oauthProvider.getAuthType() == AuthType.PKCE) {
        String state = UUID.randomUUID().toString();
        String codeVerifier = generateCodeVerifier();
        String codeChallenge = generateCodeChallenge(codeVerifier);

        oauthStore.writePKCECodeVerifier(provider, state, codeVerifier);
        response += String.format("&state=%s&code_challenge=%s&code_challenge_method=S256", state, codeChallenge);
      }

      responder.sendString(response);
    });
  }

  @PUT
  @Path(API_VERSION + "/oauth/provider/{provider}")
  public void putOAuthProvider(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("provider") String oauthProvider,
                               @QueryParam("reuse_client_credentials") @DefaultValue("false")
                               Boolean reuseClientCredentials) {
    respond(responder, () -> {
      contextAccessEnforcer.enforce(
          NamespaceId.SYSTEM.secureKey(OAuthStore.getClientCredsKey(oauthProvider)),
          StandardPermission.UPDATE);
      PutOAuthProviderRequest putOAuthProviderRequest = GSON.fromJson(
          StandardCharsets.UTF_8.decode(request.getContent()).toString(),
          PutOAuthProviderRequest.class);
      CredentialEncodingStrategy strategy = putOAuthProviderRequest.getCredentialEncodingStrategy();
      String userAgent = putOAuthProviderRequest.getUserAgent();
      
      // Validate URLs
      URL loginURL = validateOAuthUrl(putOAuthProviderRequest.getLoginURL(), false);
      URL tokenRefreshURL = validateOAuthUrl(putOAuthProviderRequest.getTokenRefreshURL(), false);

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

      if (provider.getRefreshType() == RefreshType.RTR) {
        if (!oauthStore.getStoreInfo().getCapabilities().contains(SecureStoreInfo.Capability.SECRET_LEASING)) {
          throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
              "The Secure Store backend does not support Refresh Token Rotation (RTR).");
        }
      }

      oauthStore.writeProvider(provider, reuseClientCredentials);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  @DELETE
  @Path(API_VERSION + "/oauth/provider/{provider}")
  public void deleteOAuthProvider(HttpServiceRequest request, HttpServiceResponder responder,
                                  @PathParam("provider") String oauthProvider,
                                  @QueryParam("preserve_client_credentials") @DefaultValue("false")
                                  boolean preserveClientCredentials) {
    respond(responder, () -> {
      contextAccessEnforcer.enforce(
          NamespaceId.SYSTEM.secureKey(OAuthStore.getClientCredsKey(oauthProvider)),
          StandardPermission.DELETE);
      oauthStore.deleteProvider(oauthProvider, preserveClientCredentials);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  @PUT
  @Path(API_VERSION + "/oauth/provider/{provider}/credential/{credential}")
  public void putOAuthCredential(HttpServiceRequest request, HttpServiceResponder responder,
                                 @PathParam("provider") String provider,
                                 @PathParam("credential") String credentialId) {
    respond(responder, () -> {
      contextAccessEnforcer.enforce(
          NamespaceId.SYSTEM.secureKey(OAuthStore.getRefreshTokenKey(provider, credentialId)),
          StandardPermission.UPDATE);
      PutOAuthCredentialRequest putOAuthCredentialRequest = GSON.fromJson(
          StandardCharsets.UTF_8.decode(request.getContent()).toString(),
          PutOAuthCredentialRequest.class);

      if (Strings.isNullOrEmpty(putOAuthCredentialRequest.getOneTimeCode())) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid request: missing one-time code");
      }
      if (Strings.isNullOrEmpty(putOAuthCredentialRequest.getRedirectURI())) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid request: missing redirect URI");
      }

      OAuthProvider oauthProvider = getProvider(provider);
      String state = putOAuthCredentialRequest.getState();
      String codeVerifier = null;
      
      if (oauthProvider.getAuthType() == AuthType.PKCE) {
        if (Strings.isNullOrEmpty(state) || !state.matches("^[a-zA-Z0-9-]+$")) {
          throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST, 
              "State is required and must be valid for PKCE authentication");
        }
        codeVerifier = oauthStore.getPKCECodeVerifier(provider, state);
        oauthStore.deletePKCECodeVerifier(provider, state);
      }

      OAuthHttpResponse response;
      try {
        response = executeOAuthRequest(createGetRefreshTokenRequest(
            oauthProvider,
            putOAuthCredentialRequest.getOneTimeCode(),
            putOAuthCredentialRequest.getRedirectURI(),
            codeVerifier));
      } catch (IOException e) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Error while fetching refresh token", e);
      }

      if (response.getResponseCode() != 200) {
        throw new OAuthServiceException(response.getResponseCode(),
            "Request for refresh token did not return 200. Response code: " + response.getResponseCode()
            + " , response message: " + response.getResponseMessage());
      }

      RefreshTokenResponse refreshTokenResponse;
      try {
        refreshTokenResponse = GSON.fromJson(response.getResponseBodyAsString(), RefreshTokenResponse.class);
      } catch (JsonSyntaxException e) {
        throw new OAuthServiceException(
            HttpURLConnection.HTTP_BAD_REQUEST, "Refresh token response is not valid JSON.", e);
      }

      boolean hasRefreshToken = refreshTokenResponse != null
          && !Strings.isNullOrEmpty(refreshTokenResponse.getRefreshToken());
      boolean hasAccessToken = refreshTokenResponse != null
          && !Strings.isNullOrEmpty(refreshTokenResponse.getAccessToken());

      if (!hasAccessToken && !hasRefreshToken) {
        throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
            "Refresh token response is missing the required access token or refresh token.");
      }

      if (hasRefreshToken) {
        writeRefreshToken(provider, credentialId, refreshTokenResponse.getRefreshToken(),
            putOAuthCredentialRequest.getRedirectURI());
      }
      // For standard flow, if there no refresh token, store access token (generally long lived)
      // For RTR, also store the initial Access Token in OAuthStore
      if (RefreshType.RTR.equals(oauthProvider.getRefreshType()) || !hasRefreshToken) {
        writeAccessToken(oauthProvider, provider, credentialId, refreshTokenResponse);
      }

      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  @GET
  @Path(API_VERSION + "/oauth/provider/{provider}/credential/{credential}")
  public void getOAuthCredential(HttpServiceRequest request, HttpServiceResponder responder,
                                 @PathParam("provider") String provider,
                                 @PathParam("credential") String credentialId) {
    respond(responder, () -> {
      contextAccessEnforcer.enforce(
          NamespaceId.SYSTEM.secureKey(OAuthStore.getRefreshTokenKey(provider, credentialId)),
          StandardPermission.GET);
      OAuthProvider oauthProvider = getProvider(provider);

      // 1. Check if a valid cached access token is already available
      Optional<GetAccessTokenResponse> cachedToken = getCachedAccessTokenIfValid(
          oauthProvider, provider, credentialId);
      if (cachedToken.isPresent()) {
        responder.sendString(GSON.toJson(cachedToken.get()));
        return;
      }

      // 2. Refresh token
      GetAccessTokenResponse response;
      if (RefreshType.RTR.equals(oauthProvider.getRefreshType())) {
        // via RTR leasing
        response = fetchOAuthCredentialWithRefreshTokenRotation(oauthProvider, provider, credentialId);
      } else {
        // via Standard refresh
        RefreshTokenResponse tokenResponse = executeTokenRefresh(oauthProvider, provider, credentialId);
        response = new GetAccessTokenResponse(tokenResponse.getAccessToken(), tokenResponse.getInstanceURL());
      }
      responder.sendString(GSON.toJson(response));
    });
  }

  private RefreshTokenResponse executeTokenRefresh(OAuthProvider oauthProvider, String provider, String credentialId)
      throws Exception {
    OAuthRefreshToken refreshToken = getRefreshToken(provider, credentialId);

    OAuthHttpResponse response;
    try {
      response = executeOAuthRequest(createGetAccessTokenRequest(oauthProvider, refreshToken.getRefreshToken()));
    } catch (IOException e) {
      throw new OAuthServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR,
          "Failed to fetch access token from provider", e);
    }

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new OAuthServiceException(response.getResponseCode(),
          "Request for access token did not return 200. Response code: " + response.getResponseCode()
              + " , response message: " + response.getResponseMessage(),
          null, true);
    }

    RefreshTokenResponse tokenResponse;
    try {
      tokenResponse = GSON.fromJson(response.getResponseBodyAsString(), RefreshTokenResponse.class);
    } catch (JsonSyntaxException e) {
      throw new OAuthServiceException(
          HttpURLConnection.HTTP_BAD_REQUEST, "Access token response is not valid JSON.", e, true);
    }

    if (tokenResponse == null || Strings.isNullOrEmpty(tokenResponse.getAccessToken())) {
      throw new OAuthServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
          "Access token response body does not have access token.",
          null, true);
    }
    // If provider returned a rotated refresh token, persist the updated refresh token
    String newRefreshToken = tokenResponse.getRefreshToken();
    if (!Strings.isNullOrEmpty(newRefreshToken) && !newRefreshToken.equals(refreshToken.getRefreshToken())) {
      writeRefreshToken(provider, credentialId, newRefreshToken, refreshToken.getRedirectURI());
    }

    return tokenResponse;
  }

  /**
   * Refreshes OAuth credentials using Refresh Token Rotation (RTR) under a distributed lease.
   * Ensures only one worker refreshes the single-use refresh token at a time while concurrent
   * requests wait and poll for the newly published access token.
   *
   * @param oauthProvider the OAuth provider configuration
   * @param provider the provider identifier
   * @param credentialId the credential identifier
   * @return the refreshed access token response
   * @throws Exception if lease acquisition, token refresh, or waiting fails
   */
  private GetAccessTokenResponse fetchOAuthCredentialWithRefreshTokenRotation(OAuthProvider oauthProvider,
                                                                              String provider,
                                                                              String credentialId)
      throws Exception {
    // 1. Generate a unique lease holder ID for this request thread and try to acquire lease
    String leaseHolderId = Thread.currentThread().getId() + "-" + UUID.randomUUID();
    boolean leaseAcquired = oauthStore.acquireLease(provider, credentialId, leaseExpirationTimeoutMs, leaseHolderId);

    try {
      // 3. If Lease is held by another process -> wait for published token
      if (!leaseAcquired) {
        LOG.info("Lease is held by another process for provider {} credential {}. Waiting for new access token...",
                 provider, credentialId);
        Optional<GetAccessTokenResponse> waitedResponse = waitForNewAccessToken(oauthProvider, provider, credentialId);
        if (waitedResponse.isPresent()) {
          return waitedResponse.get();
        }

        // Timeout occurred while waiting for winner -> Attempt to acquire lease again!
        leaseAcquired = oauthStore.acquireLease(provider, credentialId, leaseExpirationTimeoutMs, leaseHolderId);
        if (!leaseAcquired) {
          throw new OAuthServiceException(HttpURLConnection.HTTP_CLIENT_TIMEOUT,
                                          "Timed out waiting for OAuth access token refresh for " + credentialId);
        }
      }

      // 4. Winner (either initial or fallback after timeout) executes token refresh and persistence
      RefreshTokenResponse tokenResponse = executeTokenRefresh(oauthProvider, provider, credentialId);
      writeAccessToken(oauthProvider, provider, credentialId, tokenResponse);

      return new GetAccessTokenResponse(tokenResponse.getAccessToken(), tokenResponse.getInstanceURL());
    } finally {
      if (leaseAcquired) {
        try {
          oauthStore.releaseLease(provider, credentialId, leaseHolderId);
        } catch (Exception e) {
          LOG.warn("Failed to release lease for provider {} credential {}: {}",
                   provider, credentialId, e.getMessage());
        }
      }
    }
  }

  private Optional<GetAccessTokenResponse> waitForNewAccessToken(OAuthProvider oauthProvider,
                                                                 String provider,
                                                                 String credentialId)
      throws OAuthServiceException {
    long deadline = System.currentTimeMillis() + leaseTakeoverTimeoutMs;

    while (System.currentTimeMillis() < deadline) {
      Optional<GetAccessTokenResponse> accessToken = getCachedAccessTokenIfValid(
          oauthProvider, provider, credentialId);
      if (accessToken.isPresent()) {
        return accessToken;
      }
      Uninterruptibles.sleepUninterruptibly(accessTokenPollIntervalMs, TimeUnit.MILLISECONDS);
    }

    return Optional.empty();
  }

  private void writeRefreshToken(String provider, String credentialId,
                                 String refreshToken, String redirectURI) throws OAuthStoreException {
    OAuthRefreshToken token = OAuthRefreshToken.newBuilder()
        .withRefreshToken(refreshToken)
        .withRedirectURI(redirectURI)
        .build();
    oauthStore.writeRefreshToken(provider, credentialId, token);
  }

  private void writeAccessToken(OAuthProvider oauthProvider, String provider, String credentialId,
                                RefreshTokenResponse tokenResponse) throws OAuthStoreException {
    if (Strings.isNullOrEmpty(tokenResponse.getAccessToken())) {
      return;
    }

    long expiresInSeconds = tokenResponse.getExpiresIn();
    long expiresAt = 0L;
    if (expiresInSeconds > 0) {
      expiresAt = System.currentTimeMillis() + (expiresInSeconds * 1000L);
    }

    String identityUrl = tokenResponse.getId();
    if (!Strings.isNullOrEmpty(identityUrl)) {
      try {
        URL validatedIdUrl = validateOAuthUrl(identityUrl, false);
        if (!OAuthStore.hasSameOrigin(validatedIdUrl.toString(), oauthProvider.getTokenRefreshURL())
            && !OAuthStore.hasSameOrigin(validatedIdUrl.toString(), oauthProvider.getLoginURL())) {
          LOG.warn("Ignoring identity URL {} with origin mismatch for provider {}",
              identityUrl, oauthProvider.getName());
          identityUrl = null;
        } else {
          identityUrl = validatedIdUrl.toString();
        }
      } catch (Exception e) {
        LOG.warn("Ignoring invalid identity URL {} for provider {}: {}",
            identityUrl, oauthProvider.getName(), e.getMessage());
        identityUrl = null;
      }
    }

    OAuthAccessToken accessToken = OAuthAccessToken.newBuilder()
        .withAccessToken(tokenResponse.getAccessToken())
        .withExpiresAt(expiresAt)
        .withIdentityUrl(identityUrl)
        .build();

    oauthStore.writeAccessToken(provider, credentialId, accessToken);
  }

  private Optional<GetAccessTokenResponse> getCachedAccessTokenIfValid(OAuthProvider oauthProvider,
      String provider,
      String credentialId)
      throws OAuthServiceException {
    Optional<OAuthAccessToken> oAuthAccessToken = getAccessToken(provider, credentialId);
    if (!oAuthAccessToken.isPresent()) {
      return Optional.empty();
    }

    if (RefreshType.RTR.equals(oauthProvider.getRefreshType())) {
      if (isAccessTokenValid(oauthProvider, oAuthAccessToken.get())) {
        LOG.debug("Returning valid cached access token for provider {} credential {}", provider, credentialId);
        return Optional.of(new GetAccessTokenResponse(oAuthAccessToken.get().getAccessToken(), ""));
      }
      return Optional.empty();
    }

    // Standard flow: permanent token stored without refresh token
    return Optional.of(new GetAccessTokenResponse(oAuthAccessToken.get().getAccessToken(), ""));
  }

  private boolean isAccessTokenValid(OAuthProvider oauthProvider, OAuthAccessToken token) {
    // Rule 1: If expiresAt is present (> 0), use it with configured safety buffer
    if (token.getExpiresAt() > 0) {
      return !token.isExpired(accessTokenRefreshBufferMs);
    }

    // Rule 2: Else if identityUrl is present, validate against identity URL
    String idUrl = token.getIdentityUrl();
    if (!Strings.isNullOrEmpty(idUrl)) {
      try {
        URL validatedIdUrl = validateOAuthUrl(idUrl, true);
        if (!OAuthStore.hasSameOrigin(validatedIdUrl.toString(), oauthProvider.getTokenRefreshURL())
            && !OAuthStore.hasSameOrigin(validatedIdUrl.toString(), oauthProvider.getLoginURL())) {
          LOG.warn("Refusing to validate access token against identity URL {} with origin mismatch "
              + "for provider {}", idUrl, oauthProvider.getName());
          return false;
        }
        HttpRequest request = HttpRequest.get(validatedIdUrl)
            .addHeader("Authorization", "Bearer " + token.getAccessToken())
            .build();
        OAuthHttpResponse response = executeOAuthRequest(request);
        return response.getResponseCode() == HttpURLConnection.HTTP_OK;
      } catch (Exception e) {
        LOG.warn("Failed to validate access token via identity URL {}: {}",
            idUrl, e.getMessage());
        return false;
      }
    }

    return false;
  }

  @GET
  @Path(API_VERSION + "/oauth/provider/{provider}/credential/{credential}/valid")
  public void getOAuthCredentialValidity(HttpServiceRequest request, HttpServiceResponder responder,
                                         @PathParam("provider") String provider,
                                         @PathParam("credential") String credentialId) {
    respond(responder, () -> {
      contextAccessEnforcer.enforce(
          NamespaceId.SYSTEM.secureKey(OAuthStore.getRefreshTokenKey(provider, credentialId)),
          StandardPermission.GET);
      OAuthProvider oauthProvider = getProvider(provider);

      // 1. Check if a valid cached access token is already available
      Optional<GetAccessTokenResponse> cachedToken = getCachedAccessTokenIfValid(
          oauthProvider, provider, credentialId);
      if (cachedToken.isPresent()) {
        responder.sendString(GSON.toJson(new CredentialIsValidResponse(true)));
        return;
      }

      // 2. Verify credential by refreshing token (via RTR leasing or Standard refresh)
      try {
        if (RefreshType.RTR.equals(oauthProvider.getRefreshType())) {
          fetchOAuthCredentialWithRefreshTokenRotation(oauthProvider, provider, credentialId);
        } else {
          executeTokenRefresh(oauthProvider, provider, credentialId);
        }
        responder.sendString(GSON.toJson(new CredentialIsValidResponse(true)));
      } catch (OAuthServiceException e) {
        if (e.isProviderError()) {
          responder.sendString(GSON.toJson(new CredentialIsValidResponse(false)));
          return;
        }
        throw e;
      }
    });
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
                                  OAuthClientCredentials clientCreds,
                                  String codeVerifier) {
    String body;
    switch (strategy) {
      case BASIC_AUTH:
        if (grantType.equals("authorization_code")) {
          body = String.format("code=%s&redirect_uri=%s&grant_type=%s", code, redirectURI, grantType);
        } else {
          body = String.format("grant_type=%s&refresh_token=%s", grantType, refreshToken);
        }
        break;
      case FORM_BODY: // fall-through
      default:
        if (grantType.equals("authorization_code")) {
          body = String.format("code=%s&redirect_uri=%s&client_id=%s&client_secret=%s&grant_type=%s",
              code, redirectURI, clientCreds.getClientId(), clientCreds.getClientSecret(), grantType);
        } else {
          body = String.format("grant_type=%s&client_id=%s&client_secret=%s&refresh_token=%s",
              grantType, clientCreds.getClientId(), clientCreds.getClientSecret(), refreshToken);
        }
        break;
    }
    if (!Strings.isNullOrEmpty(codeVerifier)) {
      body += "&code_verifier=" + codeVerifier;
    }
    return body;
  }

  /** Build HTTP request for getting tokens */
  private HttpRequest.Builder buildHttpRequest(String body,
                                               CredentialEncodingStrategy strategy,
                                               OAuthClientCredentials clientCreds,
                                               String refreshTokenURL,
                                               boolean addContentType,
                                               String userAgent) throws MalformedURLException {
    URL url = validateOAuthUrl(refreshTokenURL, true);
    HttpRequest.Builder requestBuilder = HttpRequest.post(url)
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

  private static OAuthHttpResponse executeOAuthRequest(HttpRequest request) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) request.getURL().openConnection();
    try {
      connection.setInstanceFollowRedirects(false);
      connection.setRequestMethod(request.getMethod().name());
      connection.setConnectTimeout(HttpRequestConfig.DEFAULT.getConnectTimeout());
      connection.setReadTimeout(HttpRequestConfig.DEFAULT.getReadTimeout());
      if (request.getHeaders() != null) {
        for (Map.Entry<String, String> header : request.getHeaders().entries()) {
          connection.setRequestProperty(header.getKey(), header.getValue());
        }
      }
      ContentProvider<? extends InputStream> body = request.getBody();
      if (body != null) {
        connection.setDoOutput(true);
        try (InputStream input = body.getInput();
             OutputStream output = connection.getOutputStream()) {
          ByteStreams.copy(input, output);
        }
      }
      int responseCode = connection.getResponseCode();
      String responseMessage = connection.getResponseMessage();
      InputStream responseStream = responseCode >= 200 && responseCode < 300
          ? connection.getInputStream()
          : connection.getErrorStream();
      String responseBody = "";
      if (responseStream != null) {
        try (InputStream stream = responseStream) {
          responseBody = new String(ByteStreams.toByteArray(stream), StandardCharsets.UTF_8);
        }
      }
      return new OAuthHttpResponse(responseCode, responseMessage, responseBody);
    } finally {
      connection.disconnect();
    }
  }

  private URL validateOAuthUrl(String urlStr, boolean requireResolvable) throws MalformedURLException {
    URL url = new URL(urlStr);
    URI uri;
    try {
      uri = url.toURI();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid OAuth URL: " + urlStr, e);
    }
    String scheme = url.getProtocol();
    if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
      throw new IllegalArgumentException("Invalid OAuth URL scheme: " + scheme);
    }
    if (url.getUserInfo() != null || uri.getRawUserInfo() != null) {
      throw new IllegalArgumentException("OAuth URL must not contain userinfo");
    }
    String rawHost = url.getHost();
    if (Strings.isNullOrEmpty(rawHost) || Strings.isNullOrEmpty(uri.getHost()) || rawHost.contains("%")) {
      throw new IllegalArgumentException("Invalid OAuth URL host: " + rawHost);
    }
    String host = rawHost.startsWith("[") && rawHost.endsWith("]")
        ? rawHost.substring(1, rawHost.length() - 1) : rawHost;
    try {
      boolean allowPrivate = isAllowPrivateUrls();
      for (InetAddress address : InetAddress.getAllByName(host)) {
        validateInetAddress(address, allowPrivate);
      }
    } catch (UnknownHostException e) {
      if (requireResolvable) {
        throw new IllegalArgumentException("Unresolvable OAuth URL host: " + rawHost, e);
      }
    }
    return url;
  }

  private static void validateInetAddress(InetAddress address, boolean allowPrivate) {
    if (address.isAnyLocalAddress() || address.isLinkLocalAddress() || address.isMulticastAddress()) {
      throw new IllegalArgumentException("Disallowed OAuth URL IP address: " + address.getHostAddress());
    }
    byte[] bytes = address.getAddress();
    // 100.64.0.0/10 CGNAT: GKE may use it for pod/service ranges and the JDK does not classify it
    boolean isCgnat = bytes.length == 4 && (bytes[0] & 0xFF) == 100 && (bytes[1] & 0xC0) == 64;
    // fc00::/7 IPv6 ULA: GKE may use it for pod/service ranges and the JDK does not classify it
    boolean isUla = bytes.length == 16 && (bytes[0] & 0xFE) == 0xFC;
    if (!allowPrivate && (address.isLoopbackAddress() || address.isSiteLocalAddress() || isCgnat || isUla)) {
      throw new IllegalArgumentException("Private or loopback OAuth URL IP address is not allowed: "
          + address.getHostAddress());
    }
    if (address instanceof Inet6Address && InetAddresses.hasEmbeddedIPv4ClientAddress((Inet6Address) address)) {
      validateInetAddress(InetAddresses.getEmbeddedIPv4ClientAddress((Inet6Address) address), allowPrivate);
    }
  }

  private static final class OAuthHttpResponse {
    private final int responseCode;
    private final String responseMessage;
    private final String responseBody;

    private OAuthHttpResponse(int responseCode, @Nullable String responseMessage, String responseBody) {
      this.responseCode = responseCode;
      this.responseMessage = responseMessage;
      this.responseBody = responseBody;
    }

    int getResponseCode() {
      return responseCode;
    }

    @Nullable
    String getResponseMessage() {
      return responseMessage;
    }

    String getResponseBodyAsString() {
      return responseBody;
    }
  }

  private String generateCodeVerifier() {
    byte[] codeVerifierBytes = new byte[32];
    SECURE_RANDOM.nextBytes(codeVerifierBytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(codeVerifierBytes);
  }

  private String generateCodeChallenge(String codeVerifier) throws OAuthServiceException {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(codeVerifier.getBytes(StandardCharsets.US_ASCII));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(md.digest());
    } catch (Exception e) {
      throw new OAuthServiceException(
              HttpURLConnection.HTTP_INTERNAL_ERROR, "Failed to generate SHA-256 code challenge", e);
    }
  }

  /**
   * Build the HttpRequest to request a refresh token from the OAuth provider
   * @param provider
   * @param code the authorization code given after the user accepts OAuth from the provider
   * @param redirectURI
   */
  private HttpRequest createGetRefreshTokenRequest(OAuthProvider provider, String code, String redirectURI,
                                                   String codeVerifier)
      throws OAuthServiceException {
    OAuthClientCredentials clientCreds = provider.getClientCredentials();
    CredentialEncodingStrategy strategy = provider.getCredentialEncodingStrategy();
    String tokenRefreshURL = provider.getTokenRefreshURL();
    String body = buildRequestBody(strategy, "authorization_code", code, redirectURI, null, clientCreds, codeVerifier);
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
    String body = buildRequestBody(strategy, "refresh_token", null, null, refreshToken, clientCreds, null);
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


  @FunctionalInterface
  private interface EndpointRunnable {
    void run() throws Exception;
  }

  private void respond(HttpServiceResponder responder, EndpointRunnable runnable) {
    try {
      runnable.run();
    } catch (OAuthServiceException e) {
      e.respond(responder);
    } catch (JsonSyntaxException e) {
      sendError(responder, HttpURLConnection.HTTP_BAD_REQUEST, "Invalid JSON: " + e.getMessage(), e);
    } catch (MalformedURLException e) {
      sendError(responder, HttpURLConnection.HTTP_BAD_REQUEST, "Invalid URL: " + e.getMessage(), e);
    } catch (NullPointerException | IllegalArgumentException e) {
      sendError(responder, HttpURLConnection.HTTP_BAD_REQUEST, "Invalid request: " + e.getMessage(), e);
    } catch (OAuthStoreException e) {
      sendError(responder, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage(), e);
    } catch (Exception e) {
      if (e instanceof HttpErrorStatusProvider) {
        responder.sendError(((HttpErrorStatusProvider) e).getStatusCode(), e.getMessage());
        return;
      }
      LOG.error("An internal error has occurred", e);
      sendError(responder, HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal error", e);
    }
  }

  private void sendError(HttpServiceResponder responder, int statusCode, String message, Throwable cause) {
    new OAuthServiceException(statusCode, message, cause).respond(responder);
  }

  private static class OAuthServiceException extends Exception {

    private final int status;
    private final boolean providerError;

    OAuthServiceException(int status, String message, Throwable cause) {
      this(status, message, cause, false);
    }

    OAuthServiceException(int status, String message) {
      this(status, message, null, false);
    }

    OAuthServiceException(int status, String message, Throwable cause, boolean providerError) {
      super(message, cause);
      this.status = status;
      this.providerError = providerError;
    }

    int getStatus() {
      return this.status;
    }

    boolean isProviderError() {
      return this.providerError;
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
}
