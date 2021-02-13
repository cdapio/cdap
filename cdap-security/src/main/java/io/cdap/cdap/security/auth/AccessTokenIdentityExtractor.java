/*
 * Copyright © 2014-2021 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

import com.google.inject.Inject;
import io.cdap.cdap.common.io.Codec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Base64;

/**
 * The AccessTokenIdentityExtractor attempts to extract an {@link AccessToken} included in the Authorization header,
 * validate it, then transform it into a {@link UserIdentityPair}.
 */
public class AccessTokenIdentityExtractor implements UserIdentityExtractor {
  public static final String NAME = "AccessTokenIdentityExtractor";

  private static final Logger LOG = LoggerFactory.getLogger(AccessTokenIdentityExtractor.class);

  private final TokenValidator tokenValidator;
  private final Codec<AccessToken> accessTokenCodec;
  private final Codec<UserIdentity> userIdentityCodec;

  @Inject
  public AccessTokenIdentityExtractor(TokenValidator tokenValidator, Codec<AccessToken> accessTokenCodec,
                                      Codec<UserIdentity> userIdentityCodec) {
    this.tokenValidator = tokenValidator;
    this.accessTokenCodec = accessTokenCodec;
    this.userIdentityCodec = userIdentityCodec;
  }

  /**
   * Extracts a {@link UserIdentityPair} from an HTTP request by validating its {@link AccessToken} in the Authorization
   * header. The access token is an instance of {@link AccessToken} which is base64-serialized.
   * @param request The HTTP Request to extract the user identity from
   * @return the user identity backed by an access token
   * @throws UserIdentityExtractionException on identity extraction failure
   */
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
    TokenState state = tokenValidator.validate(accessToken);

    if (!state.isValid()) {
      return new UserIdentityExtractionResponse(UserIdentityExtractionState.ERROR_INVALID_TOKEN,
                                                String.format("Failed to validate access token with reason: %s",
                                                              state));
    }

    byte[] decodedAccessToken = Base64.getDecoder().decode(accessToken);
    try {
      AccessToken accessTokenObj = accessTokenCodec.decode(decodedAccessToken);
      UserIdentity userIdentityObj = accessTokenObj.getIdentifier();
      byte[] encodedAccessTokenIdentifier = userIdentityCodec.encode(userIdentityObj);
      UserIdentityPair pair = new UserIdentityPair(Base64.getEncoder().encodeToString(encodedAccessTokenIdentifier),
                                                   userIdentityObj);
      return new UserIdentityExtractionResponse(pair);
    } catch (IOException e) {
      // This shouldn't happen in normal case, since the token is already validated
      LOG.warn("Exception raised when getting token information from a validated token", e);
      throw new UserIdentityExtractionException("Failed to decode access token due to codec error", e);
    }
  }
}
