/*
 * Copyright © 2017 Cask Data, Inc.
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
import com.google.common.util.concurrent.AbstractService;

/**
 * Simple {@link co.cask.cdap.security.auth.TokenValidator} implementation for test cases, which always
 * returns {@link co.cask.cdap.security.auth.TokenState#MISSING} for all tokens.
 */
public class MissingTokenValidator extends AbstractService implements TokenValidator {
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
    return TokenState.MISSING;
  }
}
