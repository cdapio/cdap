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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.proto.id.NamespaceId;
import com.google.gson.Gson;

/**
 * A {@link MacroEvaluator} for resolving the {@code ${oauthStaticAccessToken(provider, credentialId)}} macro
 * function. It fetches the stored access token from the secure store.
 */
public class OAuthStaticAccessTokenMacroEvaluator implements MacroEvaluator {

  public static final String FUNCTION_NAME = "oauthStaticAccessToken";
  private static final String SERVICE_NAME = "Oauth";
  private static final String ACCESS_TOKEN_KEY_PREFIX = "oauthaccesstoken";

  private final SecureStore secureStore;
  private final String namespace = NamespaceId.SYSTEM.getNamespace();
  private final Gson gson;

  public OAuthStaticAccessTokenMacroEvaluator(SecureStore secureStore) {
    this.secureStore = secureStore;
    this.gson = new Gson();
  }

  @Override
  public String lookup(String property) throws InvalidMacroException {
    // this will get ignored by the parser
    throw new InvalidMacroException("Unable to lookup the value for " + property);
  }

  @Override
  public String evaluate(String macroFunction, String... args) throws InvalidMacroException {
    if (!FUNCTION_NAME.equals(macroFunction)) {
      // This shouldn't happen
      throw new IllegalArgumentException("Invalid function name " + macroFunction
          + ". Expecting " + FUNCTION_NAME);
    }
    if (args.length != 2) {
      throw new InvalidMacroException(
          "Macro '" + FUNCTION_NAME + "' should have exactly 2 arguments", new ErrorCategory(
          ErrorCategoryEnum.MACROS, String.format("%s-%s", SERVICE_NAME, FUNCTION_NAME)));
    }

    try {
      return getAccessToken(args[0], args[1]);
    } catch (Exception e) {
      throw new InvalidMacroException(
          "Failed to resolve macro '" + FUNCTION_NAME + "(" + args[0] + ',' + args[1] + ")'", e);
    }
  }

  /**
   * Gets the OAuth access token for the given provider and credential ID.
   *
   * @param provider the name of the OAuth provider
   * @param credentialId the ID of the authenticated credential
   * @return a string that's the value of the OAuth access token
   */
  private String getAccessToken(String provider,
      String credentialId) throws InvalidMacroException {
    String key = getAccessTokenKey(provider, credentialId);

    try {
      OAuthInfo authInfo = gson.fromJson(Bytes.toString(secureStore.getData(namespace, key)), OAuthInfo.class);
      return authInfo.accessToken;
    } catch (Exception e) {
      throw new InvalidMacroException(
          String.format("Failed to get access token '%s' in namespace '%s'", key, namespace), e);
    }
  }

  private static final class OAuthInfo {
    private final String accessToken;

    private OAuthInfo(String accessToken) {
      this.accessToken = accessToken;
    }
  }

  private static String getAccessTokenKey(String oauthProvider, String credentialId) {
    return String.format("%s-%s-%s", ACCESS_TOKEN_KEY_PREFIX, oauthProvider.toLowerCase(), credentialId.toLowerCase());
  }
}
