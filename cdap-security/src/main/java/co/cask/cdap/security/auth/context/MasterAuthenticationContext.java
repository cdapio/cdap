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

package co.cask.cdap.security.auth.context;

import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;

/**
 * An {@link AuthenticationContext} for HTTP requests in the Master. The authentication details in this context are
 * derived from {@link SecurityRequestContext}.
 *
 * @see SecurityRequestContext
 */
public class MasterAuthenticationContext implements AuthenticationContext {

  @Override
  public Principal getPrincipal() {
    return SecurityRequestContext.toPrincipal();
  }
}
