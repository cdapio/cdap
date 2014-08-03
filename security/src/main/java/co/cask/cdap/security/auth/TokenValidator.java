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

import com.google.common.util.concurrent.Service;

/**
 * Interface TokenValidator to validate the access token.
 */
public interface TokenValidator extends Service {
  /**
   * Validates the access token and returns the {@link co.cask.cdap.security.auth.TokenState}
   * describing the cause to be in this state
   *
   * @param token The token to be validated.
   * @return The state after validation.
   */
  TokenState validate(String token);
}
