/*
 * Copyright © 2015-2021 Cask Data, Inc.
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

package io.cdap.cdap.gateway.router;

import io.cdap.cdap.security.auth.TokenState;
import io.cdap.cdap.security.auth.TokenValidator;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.auth.UserIdentityExtractionException;
import io.cdap.cdap.security.auth.UserIdentityExtractionResponse;
import io.cdap.cdap.security.auth.UserIdentityExtractionState;
import io.cdap.cdap.security.auth.UserIdentityExtractor;
import io.cdap.cdap.security.auth.UserIdentityPair;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;

import java.util.LinkedHashSet;

public class MockAccessTokenIdentityExtractor implements UserIdentityExtractor {
  private final TokenValidator validator;

  public MockAccessTokenIdentityExtractor(TokenValidator validator) {
    this.validator = validator;
  }

  @Override
  public UserIdentityExtractionResponse extract(HttpRequest request) throws UserIdentityExtractionException {
    String auth = request.headers().get(HttpHeaderNames.AUTHORIZATION);
    String accessToken = null;
    if (auth != null) {
      int idx = auth.trim().indexOf(' ');
      if (idx < 0) {
        return new UserIdentityExtractionResponse(UserIdentityExtractionState.ERROR_MISSING_CREDENTIAL,
                                                  "No access token found");
      }

      accessToken = auth.substring(idx + 1).trim();
    }
    if (accessToken == null || accessToken.length() == 0) {
      return new UserIdentityExtractionResponse(UserIdentityExtractionState.ERROR_MISSING_CREDENTIAL,
                                                "No access token found");
    }
    TokenState state = validator.validate(accessToken);

    if (!state.isValid()) {
      return new UserIdentityExtractionResponse(UserIdentityExtractionState.ERROR_INVALID_TOKEN,
                                            String.format("Failed to validate access token with reason: %s", state));
    }
    UserIdentityPair pair = new UserIdentityPair(accessToken,
                                                 new UserIdentity("dummy", new LinkedHashSet<String>(),
                                                                  System.currentTimeMillis(),
                                                                  System.currentTimeMillis() + 100000));
    return new UserIdentityExtractionResponse(pair);
  }
}
