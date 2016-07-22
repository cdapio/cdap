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
import com.google.common.base.Throwables;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * An {@link AuthenticationContext} for use in program containers. The authentication details in this context are
 * determined based on the {@link UserGroupInformation} of the user running the program.
 */
public class ProgramContainerAuthenticationContext implements AuthenticationContext {

  @Override
  public Principal getPrincipal() {
    try {
      return new Principal(UserGroupInformation.getCurrentUser().getShortUserName(), Principal.PrincipalType.USER);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
