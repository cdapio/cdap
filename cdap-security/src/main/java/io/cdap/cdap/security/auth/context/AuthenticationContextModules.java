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

import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteAuthenticator;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
  public Module getProgramContainerModule(CConfiguration cConf, final String principal) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        String username = getUsername();
        bind(AuthenticationContext.class)
          .toInstance(new ProgramContainerAuthenticationContext(
            new Principal(username, Principal.PrincipalType.USER, principal, loadRemoteCredentials(cConf))));
      }
    };
  }

  /**
   * An {@link AuthenticationContext} for use in program containers. The authentication details in this context are
   * determined based on the {@link UserGroupInformation} of the user running the program.
   */
  public Module getProgramContainerModule(CConfiguration cConf) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        String username = getUsername();
        bind(AuthenticationContext.class)
          .toInstance(new ProgramContainerAuthenticationContext(new Principal(username,
                                                                              Principal.PrincipalType.USER,
                                                                              loadRemoteCredentials(cConf))));
      }
    };
  }

  private Credential loadRemoteCredentials(CConfiguration cConf) {
    Path secretFile = Paths.get(Constants.Security.Authentication.RUNTIME_TOKEN_FILE);
    if (Files.exists(secretFile)) {
      try {
        String token = new String(Files.readAllBytes(secretFile), StandardCharsets.UTF_8);
        return new Credential(token, Credential.CredentialType.INTERNAL);
      } catch (IOException e) {
        throw new IllegalStateException("Can't read runtime token file", e);
      }
    }
    String token = cConf.get(Constants.Security.Authentication.RUNTIME_TOKEN);
    return token == null ? null : new Credential(token, Credential.CredentialType.INTERNAL);
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
   *
   * @return A module with internal authentication bindings for testing.
   */
  public Module getNoOpModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(AuthenticationContext.class).to(AuthenticationTestContext.class);
      }
    };
  }

  /**
   * An {@link AuthenticationContext} and {@link RemoteAuthenticator} for HTTP requests which use internal
   * authentication. The authentication details in this context are derived from a combination of
   * {@link SecurityRequestContext} and the {@link SystemAuthenticationContext}.
   *
   * @see SecurityRequestContext
   * @see SystemAuthenticationContext
   *
   * @param cConf The configuration for the cluster
   * @return A module with bindings for internal authentication for master services.
   */
  public Module getInternalAuthMasterModule(CConfiguration cConf) {
    if (!cConf.getBoolean(Constants.Security.ENFORCE_INTERNAL_AUTH)) {
      return getMasterModule();
    }
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(AuthenticationContext.class).to(SystemAuthenticationContext.class);
        expose(AuthenticationContext.class);
      }
    };
  }

  /**
   * Authentication module for workers such as preview and task workers. The authentication details in this context are
   * derived from the {@link WorkerAuthenticationContext}.
   *
   * @see WorkerAuthenticationContext
   *
   * @param cConf The configuration for the cluster
   * @return A module with bindings for internal authentication for worker services.
   */
  public Module getInternalAuthWorkerModule(CConfiguration cConf) {
    if (!cConf.getBoolean(Constants.Security.ENFORCE_INTERNAL_AUTH)) {
      return getMasterModule();
    }
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(AuthenticationContext.class).to(WorkerAuthenticationContext.class);
      }
    };
  }
}
