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
 */

package io.cdap.cdap.etl.common;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * A {@link MacroEvaluator} for resolving the {@code ${oauth(provider, credentialId)}} macro function. It uses
 * the studio service for getting oauth access token at runtime.
 */
public class OAuthMacroEvaluator implements MacroEvaluator {

  public static final String FUNCTION_NAME = "oauth";

  private static final long OAUTH_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);
  private static final long OAUTH_RETRY_BASE_DELAY_MILLIS = 200L;
  private static final long OAUTH_RETRY_MAX_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final double OAUTH_RETRY_DELAY_MULTIPLIER = 1.2d;
  private static final double OAUTH_RETRY_RANDOMIZE_FACTOR = 0.1d;

  private final ServiceDiscoverer serviceDiscoverer;

  public OAuthMacroEvaluator(ServiceDiscoverer serviceDiscoverer) {
    this.serviceDiscoverer = serviceDiscoverer;
  }

  @Override
  public String lookup(String property) throws InvalidMacroException {
    throw new InvalidMacroException("The '" + FUNCTION_NAME
                                      + "' macro function doesn't support direct property lookup for property '"
                                      + property + "'");
  }

  /**
   * Evaluates the OAuth macro function by calling the OAuth service to exchange an OAuth token.
   *
   * @param args should contains exactly two arguments. The first one is the name of the OAuth provider, and the
   *             second argument is the credential id.
   * @return a OAuth token
   */
  @Override
  public String evaluate(String macroFunction, String... args) throws InvalidMacroException {
    if (!FUNCTION_NAME.equals(macroFunction)) {
      // This shouldn't happen
      throw new IllegalArgumentException("Invalid function name " + macroFunction
                                           + ". Expecting " + FUNCTION_NAME);
    }

    if (args.length != 2) {
      throw new InvalidMacroException("Macro '" + FUNCTION_NAME + "' should have exactly 2 arguments");
    }

    // Make call to get OAuth token with exponential delay on failure retry.
    long delay = OAUTH_RETRY_BASE_DELAY_MILLIS;
    double minMultiplier = OAUTH_RETRY_DELAY_MULTIPLIER - OAUTH_RETRY_DELAY_MULTIPLIER * OAUTH_RETRY_RANDOMIZE_FACTOR;
    double maxMultiplier = OAUTH_RETRY_DELAY_MULTIPLIER + OAUTH_RETRY_DELAY_MULTIPLIER * OAUTH_RETRY_RANDOMIZE_FACTOR;
    Stopwatch stopWatch = new Stopwatch().start();
    try {
      while (stopWatch.elapsedTime(TimeUnit.MILLISECONDS) < OAUTH_TIMEOUT_MILLIS) {
        try {
          return getOAuthToken(args[0], args[1]);
        } catch (RetryableException e) {
          TimeUnit.MILLISECONDS.sleep(delay);
          delay = (long) (delay * (minMultiplier + Math.random() * (maxMultiplier - minMultiplier + 1)));
          delay = Math.min(delay, OAUTH_RETRY_MAX_DELAY_MILLIS);
        } catch (IOException e) {
          throw new RuntimeException("Failed to get OAuth token for " + args[0] + ", " + args[1], e);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Thread interrupted while trying to get the OAuth token for "
                                   + args[0] + ", " + args[1], e);
    }
    throw new IllegalStateException("Timed out when trying to get the OAuth token for " + args[0] + ", " + args[1]);
  }

  /**
   * Gets the OAuth token for the given provider and credential ID.
   *
   * @param provider the name of the OAuth provider
   * @param credentialId the ID of the authenticated credential
   * @return an OAuth token
   * @throws IOException if failed to get the OAuth token due to non-retryable error
   * @throws RetryableException if failed to get the OAuth token due to transient error
   */
  private String getOAuthToken(String provider, String credentialId) throws IOException, RetryableException {
    HttpURLConnection urlConn = serviceDiscoverer.openConnection(NamespaceId.SYSTEM.getNamespace(),
                                                                 Constants.PIPELINEID,
                                                                 Constants.STUDIO_SERVICE_NAME,
                                                                 String.format("v1/oauth/provider/%s/credential/%s",
                                                                               provider, credentialId));
    if (urlConn == null) {
      throw new RetryableException("OAuth service is not available");
    }

    int responseCode = urlConn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      switch (responseCode) {
        case HttpURLConnection.HTTP_BAD_GATEWAY:
        case HttpURLConnection.HTTP_UNAVAILABLE:
        case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:
          throw new RetryableException("OAuth service is not available with status " + responseCode);
      }
      throw new IOException("Failed to call OAuth service with status " + responseCode + ": " + getError(urlConn));
    }

    try (Reader reader = new InputStreamReader(urlConn.getInputStream(), StandardCharsets.UTF_8)) {
      return CharStreams.toString(reader);
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Returns the full content of the error stream for the given {@link HttpURLConnection}.
   */
  private String getError(HttpURLConnection urlConn) {
    try (InputStream is = urlConn.getErrorStream()) {
      if (is == null) {
        return "Unknown error";
      }
      return new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "Unknown error due to failure to read from error output: " + e.getMessage();
    }
  }
}
