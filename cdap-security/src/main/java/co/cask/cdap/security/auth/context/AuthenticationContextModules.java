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
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Exposes the right {@link AuthenticationContext} via an {@link AbstractModule} based on the context in which
 * it is being invoked.
 */
public class AuthenticationContextModules {
  /**
   * An {@link AuthenticationContext} for HTTP requests in Master. The authentication details in this context are
   * derived from {@link SecurityRequestContext}.
   *
   * @see SecurityRequestContext
   */
  public Module getMasterModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(AuthenticationContext.class).to(MasterAuthenticationContext.class);
      }
    };
  }

  /**
   * An {@link AuthenticationContext} for use in program containers. The authentication details in this context are
   * determined based on the {@link UserGroupInformation} of the user running the program. The provided
   * kerberos principal information is also included in the {@link Principal}.
   */
  public Module getProgramContainerModule(final String principal) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        String username = getUsername();
        bind(AuthenticationContext.class)
          .toInstance(new ProgramContainerAuthenticationContext(new Principal(username,
                                                                              Principal.PrincipalType.USER,
                                                                              principal)));
      }
    };
  }

  /**
   * An {@link AuthenticationContext} for use in program containers. The authentication details in this context are
   * determined based on the {@link UserGroupInformation} of the user running the program.
   */
  public Module getProgramContainerModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        String username = getUsername();
        bind(AuthenticationContext.class)
          .toInstance(new ProgramContainerAuthenticationContext(new Principal(username,
                                                                              Principal.PrincipalType.USER)));
      }
    };
  }

  private String getUsername() {
    try {
      return UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * An {@link AuthenticationContext} for use in tests that do not need authentication/authorization. The
   * authentication details in this context are determined based on the {@link System#props user.name} system property.
   */
  public Module getNoOpModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(AuthenticationContext.class).to(AuthenticationTestContext.class);
      }
    };
  }
}
