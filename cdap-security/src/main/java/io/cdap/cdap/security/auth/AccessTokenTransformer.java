/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

import java.io.IOException;
import java.util.Base64;

/**
 * It takes the access token and transforms it to Access Token Identifier.
 */
public class AccessTokenTransformer implements TokenTransformer {
  private final Codec<AccessToken> accessTokenCodec;
  private final Codec<AccessTokenIdentifier> accessTokenIdentifierCodec;

  @Inject
  public AccessTokenTransformer(Codec<AccessToken> accessTokenCodec,
                                Codec<AccessTokenIdentifier> accessTokenIdentifierCodec) {
    this.accessTokenCodec = accessTokenCodec;
    this.accessTokenIdentifierCodec = accessTokenIdentifierCodec;
  }

  /**
   *
   * @param accessToken is the access token from Authorization header in HTTP Request
   * @return the serialized access token identifer
   * @throws IOException
   */
  @Override
  public AccessTokenIdentifierPair transform(String accessToken) throws IOException {
    byte[] decodedAccessToken = Base64.getDecoder().decode(accessToken);
    AccessToken accessTokenObj = accessTokenCodec.decode(decodedAccessToken);
    AccessTokenIdentifier accessTokenIdentifierObj = accessTokenObj.getIdentifier();
    byte[] encodedAccessTokenIdentifier = accessTokenIdentifierCodec.encode(accessTokenIdentifierObj);
    return new AccessTokenIdentifierPair(Base64.getEncoder().encodeToString(encodedAccessTokenIdentifier).trim(),
                                         accessTokenIdentifierObj);
  }
}
