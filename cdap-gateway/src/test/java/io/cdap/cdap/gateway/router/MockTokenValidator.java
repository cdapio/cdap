/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.security.auth.TokenState;
import co.cask.cdap.security.auth.TokenValidator;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;

/**
 * Simple {@link TokenValidator} implementation for test cases, which always returns
 * {@link TokenState#INVALID} for a specific token, and {@link TokenState#VALID} for
 * all other tokens.
 */
public class MockTokenValidator extends AbstractService implements TokenValidator {

  private final String tokenToFail;

  public MockTokenValidator(String tokenToFail) {
    Preconditions.checkNotNull(tokenToFail);
    this.tokenToFail = tokenToFail;
  }

  @Override
  protected void doStart() {
    notifyStarted();
  }

  @Override
  protected void doStop() {
    notifyStopped();
  }

  @Override
  public TokenState validate(String token) {
    return tokenToFail.equals(token) ? TokenState.INVALID : TokenState.VALID;
  }
}
