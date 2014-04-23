package com.continuuity.security.auth;

import com.google.inject.Inject;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * This class validates the accessToken and returns the different states
 * of accessToken validation.
 */
public class AccessTokenValidator implements TokenValidator {
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
    AccessToken accessToken;
    State state = State.TOKEN_VALID;
    if (token == null) {
      LOG.debug("Token is missing");
      return State.TOKEN_MISSING;
    }
    byte[] decodedToken = Base64.decodeBase64(token);

    try {
      accessToken = accessTokenCodec.decode(decodedToken);
      tokenManager.validateSecret(accessToken);
    } catch (IOException ioe) {
      state = State.TOKEN_INVALID;
      LOG.debug("Unknown Schema version for Access Token.");
    } catch (InvalidTokenException ite) {
        InvalidTokenException.Reason reason = ite.getReason();
        switch(reason) {
          case INVALID:
            state = State.TOKEN_INVALID;
            break;
          case EXPIRED:
            state = State.TOKEN_EXPIRED;
            break;
          case INTERNAL:
            state = State.TOKEN_INTERNAL;
            break;
        }
        LOG.debug(state.getMsg());
    }
    return state;
  }
}

