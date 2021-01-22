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

import java.io.IOException;
import java.util.LinkedHashSet;

/**
 * Transforms the passed-in token into an {@link AccessTokenIdentifierPair} by creating a new
 * {@link AccessTokenIdentifier} and setting the principal name to the token string.
 */
public class PassthroughTokenTransformer implements TokenTransformer {
  public PassthroughTokenTransformer() {}

  /**
   * @param accessToken is the access token from Authorization header in HTTP Request
   * @return the serialized access token identifer with the accessToken string as the principal name
   * @throws IOException
   */
  @Override
  public AccessTokenIdentifierPair transform(String accessToken) {
    long now = System.currentTimeMillis();
    AccessTokenIdentifier accessTokenIdentifierObj = new AccessTokenIdentifier(accessToken, new LinkedHashSet<String>(),
                                                                               now, now + 1000);
    return new AccessTokenIdentifierPair(accessToken, accessTokenIdentifierObj);
  }
}
