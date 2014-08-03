/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.security.auth;

import co.cask.cdap.common.io.Codec;
import com.google.inject.Inject;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;

/**
 * It takes the access token and transforms it to Access Token Identifier.
 */
public class AccessTokenTransformer {
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
  public AccessTokenIdentifierPair transform(String accessToken) throws IOException {
    byte[] decodedAccessToken = Base64.decodeBase64(accessToken);
    AccessToken accessTokenObj = accessTokenCodec.decode(decodedAccessToken);
    AccessTokenIdentifier accessTokenIdentifierObj = accessTokenObj.getIdentifier();
    byte[] encodedAccessTokenIdentifier = accessTokenIdentifierCodec.encode(accessTokenIdentifierObj);
    return new AccessTokenIdentifierPair(Base64.encodeBase64String(encodedAccessTokenIdentifier).trim(),
                                         accessTokenIdentifierObj);
  }

  /**
   * Access token identifier pair that has marshalled and unmarshalled
   * access token object
   */
  public class AccessTokenIdentifierPair {
    private final String accessTokenIdentifierStr;
    private final AccessTokenIdentifier accessTokenIdentifierObj;

    public AccessTokenIdentifierPair(String accessTokenIdentifierStr, AccessTokenIdentifier accessTokenIdentifierObj) {
      this.accessTokenIdentifierObj = accessTokenIdentifierObj;
      this.accessTokenIdentifierStr = accessTokenIdentifierStr;
    }

    public String getAccessTokenIdentifierStr() {
      return accessTokenIdentifierStr;
    }

    public AccessTokenIdentifier getAccessTokenIdentifierObj() {
      return accessTokenIdentifierObj;
    }

    @Override
    public String toString() {
      return accessTokenIdentifierStr;
    }
  }

}
