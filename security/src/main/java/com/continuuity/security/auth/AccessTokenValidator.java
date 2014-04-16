package com.continuuity.security.auth;

import com.google.inject.Inject;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class validates the accessToken and returns the different states
 * of accessToken validation.
 */
public class AccessTokenValidator implements Validator {
  private static final Logger LOG = LoggerFactory.getLogger(AccessTokenValidator.class);
  private final TokenManager tokenManager;
  private final Codec<AccessToken> accessTokenCodec;

 @Inject
  public AccessTokenValidator(TokenManager tokenManager, Codec<AccessToken> accessTokenCodec) {
    this.tokenManager = tokenManager;
    this.accessTokenCodec = accessTokenCodec;
  }

  /**
   *
   * @param token The access token to be validated.
   * @return The state after validation
   */
  @Override
  public State validate(String token) {
    State state = State.TOKEN_VALID;
    if (token == null) {
      LOG.debug("Token is missing");
      return State.TOKEN_MISSING;
    }
    byte[] decodedToken = Base64.decodeBase64(token);
    try {
      AccessToken accessToken = accessTokenCodec.decode(decodedToken);
      tokenManager.validateSecret(accessToken);
    } catch (Exception e) {
      System.out.println(e.getMessage());
        LOG.debug("Token is invalid " + e);
        return State.TOKEN_INVALID;
    }
    return state;
  }
}

