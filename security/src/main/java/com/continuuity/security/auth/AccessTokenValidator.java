package com.continuuity.security.auth;

import com.continuuity.common.io.Codec;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class validates the accessToken and returns the different states
 * of accessToken validation.
 */
public class AccessTokenValidator extends AbstractIdleService implements TokenValidator {
  private static final Logger LOG = LoggerFactory.getLogger(AccessTokenValidator.class);
  private final TokenManager tokenManager;
  private final Codec<AccessToken> accessTokenCodec;

  @Inject
  public AccessTokenValidator(TokenManager tokenManager, Codec<AccessToken> accessTokenCodec) {
    this.tokenManager = tokenManager;
    this.accessTokenCodec = accessTokenCodec;
  }

  @Override
  protected void startUp() throws Exception {
    tokenManager.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    tokenManager.stopAndWait();
  }

  @Override
  public TokenState validate(String token) {
    AccessToken accessToken;
    TokenState state = TokenState.VALID;
    if (token == null) {
      LOG.debug("Token is missing");
      return TokenState.MISSING;
    }
    byte[] decodedToken = Base64.decodeBase64(token);

    try {
      accessToken = accessTokenCodec.decode(decodedToken);
      tokenManager.validateSecret(accessToken);
    } catch (IOException ioe) {
      state = TokenState.INVALID;
      LOG.debug("Unknown Schema version for Access Token. {}", ioe);
    } catch (InvalidTokenException ite) {
      state = ite.getReason();
      LOG.debug("{} {}", state, ite);
    }
    return state;
  }
}
