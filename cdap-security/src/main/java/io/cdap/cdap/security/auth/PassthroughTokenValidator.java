/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a token validator which simply passes the token through and succeeds on all non-empty tokens.
 */
public class PassthroughTokenValidator extends AbstractIdleService implements TokenValidator {
  private static final Logger LOG = LoggerFactory.getLogger(PassthroughTokenValidator.class);

  /**
   * Empty constructor for a passthrough token validator instance
   */
  public PassthroughTokenValidator() {}

  @Override
  protected void startUp() {}

  @Override
  protected void shutDown() {}

  /**
   * Returns VALID if the token is non-empty.
   * @param token The token to be validated.
   * @return Whether or not the token is valid
   */
  @Override
  public TokenState validate(String token) {
    TokenState state = TokenState.VALID;
    if (token == null) {
      LOG.debug("Token is missing");
      return TokenState.MISSING;
    }
    return state;
  }
}
