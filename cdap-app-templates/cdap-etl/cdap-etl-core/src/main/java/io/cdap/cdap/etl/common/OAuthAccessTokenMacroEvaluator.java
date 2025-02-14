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

import com.google.gson.Gson;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;

/**
 * A {@link MacroEvaluator} for resolving the {@code ${oauth(provider, credentialId)}} macro
 * function. It uses the studio service for getting oauth access token at runtime.
 */
public class OAuthAccessTokenMacroEvaluator extends AbstractServiceRetryableMacroEvaluator {

  public static final String FUNCTION_NAME = "oauthAccessToken";
  private static final String SERVICE_NAME = "Oauth";

  private final ServiceDiscoverer serviceDiscoverer;
  private final Gson gson;

  public OAuthAccessTokenMacroEvaluator(ServiceDiscoverer serviceDiscoverer) {
    super(SERVICE_NAME, FUNCTION_NAME);
    this.serviceDiscoverer = serviceDiscoverer;
    this.gson = new Gson();
  }

  @Override
  public Map<String, String> evaluateMacroMap(
      String macroFunction, String... args)
      throws InvalidMacroException, IOException, RetryableException {
    throw new UnsupportedOperationException(
        String.format("This function %s can only be evaluated as string, use evaluateMacro instead",
            macroFunction));
  }

  /**
   * Evaluates the OAuth macro function by calling the OAuth service to exchange an OAuth token.
   *
   * @param args should contains exactly two arguments. The first one is the name of the OAuth
   *     provider, and the second argument is the credential id.
   * @return a string that's the value of the OAuth access token
   */
  @Override
  public String evaluateMacro(String macroFunction, String... args)
      throws InvalidMacroException, IOException,
      RetryableException {
    if (args.length != 2) {
      throw new InvalidMacroException(
          "Macro '" + FUNCTION_NAME + "' should have exactly 2 arguments", new ErrorCategory(
          ErrorCategoryEnum.MACROS, String.format("%s-%s", SERVICE_NAME, FUNCTION_NAME)));
    }

    return getAccessToken(args[0], args[1]);
  }

  /**
   * Gets the OAuth token for the given provider and credential ID.
   *
   * @param provider the name of the OAuth provider
   * @param credentialId the ID of the authenticated credential
   * @return a string that's the value of the OAuth access token
   * @throws IOException if failed to get the OAuth token due to non-retryable error
   * @throws RetryableException if failed to get the OAuth token due to transient error
   */
  private String getAccessToken(String provider,
      String credentialId) throws IOException, RetryableException {
    HttpURLConnection urlConn = serviceDiscoverer.openConnection(NamespaceId.SYSTEM.getNamespace(),
        Constants.PIPELINEID,
        Constants.STUDIO_SERVICE_NAME,
        String.format("v1/oauth/provider/%s/credential/%s",
            provider, credentialId));
    OAuthInfo authInfo = gson.fromJson(validateAndRetrieveContent(SERVICE_NAME, urlConn),
        OAuthInfo.class);
    return authInfo.accessToken;
  }

  private static final class OAuthInfo {

    private final String accessToken;

    private OAuthInfo(String accessToken) {
      this.accessToken = accessToken;
    }
  }
}
