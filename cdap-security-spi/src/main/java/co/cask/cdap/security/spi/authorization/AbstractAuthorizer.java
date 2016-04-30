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

package co.cask.cdap.security.spi.authorization;

/**
 * Abstract class that implements {@link Authorizer} and provides default no-op implementations of
 * {@link Authorizer#initialize(AuthorizationContext)} and {@link Authorizer#destroy()} so classes extending it do not
 * have to implement these methods unless necessary.
 */
public abstract class AbstractAuthorizer implements Authorizer {

  /**
   * Default no-op implementation of {@link Authorizer#initialize(AuthorizationContext)}.
   */
  public void initialize(AuthorizationContext context) throws Exception {
    // default no-op implementation
  }

  /**
   * Default no-op implementation of {@link Authorizer#destroy()}.
   */
  public void destroy() throws Exception {
    // default no-op implementation
  }
}
