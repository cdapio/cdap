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

package io.cdap.cdap.security.auth.context;

import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * An {@link AuthenticationContext} for use in program containers. The authentication details in this context are
 * determined based on the {@link UserGroupInformation} of the user running the program.
 */
class ProgramContainerAuthenticationContext implements AuthenticationContext {
  private final Principal principal;

  ProgramContainerAuthenticationContext(Principal principal) {
    this.principal = principal;
  }

  @Override
  public Principal getPrincipal() {
    return principal;
  }
}
