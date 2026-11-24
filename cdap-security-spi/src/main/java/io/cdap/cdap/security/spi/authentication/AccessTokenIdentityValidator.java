/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.security.spi.authentication;

/**
 * Validates a Base64-encoded CDAP access token and returns the verified identity embedded in it.
 */
public interface AccessTokenIdentityValidator {

  /**
   * Validates the given encoded access token and returns its verified identity.
   *
   * @param encodedToken Base64-encoded serialized access token
   * @return verified identity contained in the token
   * @throws UnauthenticatedException if the token is missing, malformed, expired, or has an invalid
   *     signature
   */
  ValidatedTokenIdentity validate(String encodedToken) throws UnauthenticatedException;
}
