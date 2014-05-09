package com.continuuity.security.auth;

import com.continuuity.security.io.Codec;
import com.google.inject.Inject;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;

/**
 * It takes the access token and transforms it to Access Token Identifier
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
  public String transform(String accessToken) throws IOException {
    byte[] decodedAccessToken = Base64.decodeBase64(accessToken);
    AccessToken accessTokenObj = accessTokenCodec.decode(decodedAccessToken);
    AccessTokenIdentifier accessTokenIdentifierObj = accessTokenObj.getIdentifier();
    byte[] encodedAccessTokenIdentifier = accessTokenIdentifierCodec.encode(accessTokenIdentifierObj);
    return Base64.encodeBase64String(encodedAccessTokenIdentifier).trim();
  }

}
