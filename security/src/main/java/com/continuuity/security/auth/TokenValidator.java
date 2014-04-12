package com.continuuity.security.auth;
import com.google.inject.Inject;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class validates the accessToken and returns the different states
 * of accessToken validation.
 */
public class TokenValidator implements Validator {
  private TokenManager tokenManager;
  private Codec<AccessToken> accessTokenCodec;
  private static final Logger LOG = LoggerFactory.getLogger(TokenValidator.class);
  @Inject
  public TokenValidator(TokenManager tokenManager, Codec<AccessToken> accessTokenCodec) {
    this.tokenManager = tokenManager;
    this.accessTokenCodec = accessTokenCodec;
  }

  /**
   *
   * @param token The access token to be validated.
   * @return The state after validation
   */
  public State validate(String token) {
    State state = State.TOKEN_VALID;
    if (token == null) {
      state = State.TOKEN_MISSING;
      LOG.error("Token is missing");
      return state;
    }
    byte[] decodedToken = Base64.decodeBase64(token);
    try {
      AccessToken accessToken = accessTokenCodec.decode(decodedToken);
      tokenManager.validateSecret(accessToken);
    } catch (Exception e) {
        state = State.TOKEN_INVALID;
        LOG.error("Token is invalid " + e);
    }
    return state;
  }
}

