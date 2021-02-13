/*
 * Copyright © 2021 Cask Data, Inc.
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

import io.netty.handler.codec.http.HttpRequest;

/**
 * An interface for extracting a {@link UserIdentityPair} from HTTP requests.
 */
public interface UserIdentityExtractor {
  /**
   * @param request The HTTP Request to extract the user identity from
   * @return the {@link UserIdentityExtractionResponse} which contains the state and identity pair
   */
  UserIdentityExtractionResponse extract(HttpRequest request) throws UserIdentityExtractionException;
}
