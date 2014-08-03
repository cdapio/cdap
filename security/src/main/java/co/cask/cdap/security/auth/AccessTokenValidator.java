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
