package com.continuuity.security.auth;
import com.google.common.base.Charsets;
import com.google.inject.Inject;
import org.apache.commons.codec.binary.Base64;


/**
 * This class validates the accessToken and returns the different states
 * of accessToken validation.
 */
public class TokenValidator implements Validator{
  private TokenManager tokenManager;
  private Codec<AccessToken> accessTokenCodec;

  @Inject
  public TokenValidator(TokenManager tokenManager, Codec<AccessToken> accessTokenCodec) {
    this.tokenManager = tokenManager;
    this.accessTokenCodec = accessTokenCodec;
  }

  public State validate(String token) {
    State state = State.TOKEN_VALID;
    if (token == null) {
      state = State.TOKEN_MISSING;
      return state;
    }
    byte[] decodedToken = Base64.decodeBase64(token);
    System.out.println(new String(decodedToken, Charsets.UTF_8));
    try {
      AccessToken accessToken = accessTokenCodec.decode(decodedToken);
      tokenManager.validateSecret(accessToken);
    } catch (Exception e) {
        state = State.TOKEN_INVALID;
    }
    return state;
  }


}

