/*
 * Copyright © 2016 Cask Data, Inc.
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

import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AuthorizationContext;

/**
 * A context that determines authentication details. {@link AccessController} extensions can obtain authentication
 * details from the {@link AuthorizationContext} available in their
 * {@link AccessController#initialize(AuthorizationContext)} method.
 */
public interface AuthenticationContext {

  /**
   * Determines the {@link Principal} making the authorization request.
   *
   * @return the {@link Principal} making the authorization request
   */
  Principal getPrincipal();
}
