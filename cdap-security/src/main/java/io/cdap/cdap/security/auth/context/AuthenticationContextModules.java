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
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.api.auditlogging.AuditLogWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpInternalAuthenticator;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Queue;

import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Exposes the right {@link AuthenticationContext} via an {@link AbstractModule} based on the
 * context in which it is being invoked.
 */
public class AuthenticationContextModules {

  /**
   * An {@link AuthenticationContext} for HTTP requests in Master. The authentication details in
   * this context are derived from a combination of {@link SecurityRequestContext} and the {@link
   * SystemAuthenticationContext}.
   *
   * @see SecurityRequestContext
   * @see SystemAuthenticationContext
   */
  public Module getMasterModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<Class<? extends AuthenticationContext>>() {
        })
            .toInstance(SystemAuthenticationContext.class);
        bind(AuthenticationContext.class).toProvider(MasterAuthenticationContextProvider.class);
        bind(InternalAuthenticator.class).toProvider(InternalAuthenticatorProvider.class);
        expose(AuthenticationContext.class);
        expose(InternalAuthenticator.class);
      }
    };
  }

  /**
   * Returns a Guice module that provides {@link AuthenticationContext} for workers such as preview
   * and task workers. The authentication details in this context are derived from the {@link
   * WorkerAuthenticationContext}.
   *
   * @see WorkerAuthenticationContext
   */
  public Module getMasterWorkerModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<Class<? extends AuthenticationContext>>() {
        })
            .toInstance(WorkerAuthenticationContext.class);
        bind(AuthenticationContext.class).toProvider(MasterAuthenticationContextProvider.class);
        bind(InternalAuthenticator.class).toProvider(InternalAuthenticatorProvider.class);
        expose(AuthenticationContext.class);
        expose(InternalAuthenticator.class);

      }
    };
  }

  /**
   * An {@link AuthenticationContext} for use in program containers. The authentication details in
   * this context are determined based on the {@link UserGroupInformation} of the user running the
   * program. The provided kerberos principal information is also included in the {@link
   * Principal}.
   */
  public Module getProgramContainerModule(CConfiguration cConf, final String principal) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        String username = getUsername();
        bind(AuthenticationContext.class)
            .toInstance(new ProgramContainerAuthenticationContext(
                new Principal(username, Principal.PrincipalType.USER, principal,
                    loadRemoteCredentials(cConf))));
        bind(InternalAuthenticator.class).toProvider(InternalAuthenticatorProvider.class);
      }
    };
  }

  /**
   * An {@link AuthenticationContext} for use in program containers. The authentication details in
   * this context are determined based on the {@link UserGroupInformation} of the user running the
   * program.
   */
  public Module getProgramContainerModule(CConfiguration cConf) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        Credential remoteCredentials = loadRemoteCredentials(cConf);
        if (remoteCredentials != null) {
          String username = getUsername();
          bind(AuthenticationContext.class)
              .toInstance(new ProgramContainerAuthenticationContext(new Principal(username,
                  Principal.PrincipalType.USER,
                  loadRemoteCredentials(cConf))));
        } else {
          bind(new TypeLiteral<Class<? extends AuthenticationContext>>() {
          })
              .toInstance(WorkerAuthenticationContext.class);
          bind(AuthenticationContext.class).toProvider(MasterAuthenticationContextProvider.class);
        }
        bind(InternalAuthenticator.class).toProvider(InternalAuthenticatorProvider.class);
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
   * An {@link AuthenticationContext} for use in tests that do not need
   * authentication/authorization. The authentication details in this context are determined based
   * on the {@code user.name} from the {@link System#getProperties()}.
   *
   * @return A module with internal authentication bindings for testing.
   */
  public Module getNoOpModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(AuthenticationContext.class).to(AuthenticationTestContext.class);
        bind(InternalAuthenticator.class).toProvider(InternalAuthenticatorProvider.class);
        bind(AuditLogWriter.class).to(NoOpAuditLogWriter.class);
      }
    };
  }

  /**
   * A {@link Provider} for {@link InternalAuthenticator} for use in the {@link
   * io.cdap.cdap.common.internal.remote.RemoteClient}.
   */
  private static final class InternalAuthenticatorProvider implements
      Provider<InternalAuthenticator> {

    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    InternalAuthenticatorProvider(CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    @Override
    public InternalAuthenticator get() {
      if (SecurityUtil.isInternalAuthEnabled(cConf)) {
        return new DefaultInternalAuthenticator(injector.getInstance(AuthenticationContext.class));
      }
      return new NoOpInternalAuthenticator();
    }
  }

  /**
   * A {@link Provider} for {@link AuthenticationContext} based on CDAP configuration for the master
   * service processes and the runtime processes to use.
   */
  private static final class MasterAuthenticationContextProvider implements
      Provider<AuthenticationContext> {

    private final CConfiguration cConf;
    private final Injector injector;
    private final Class<? extends AuthenticationContext> internalAuthContextClass;

    @Inject
    MasterAuthenticationContextProvider(CConfiguration cConf, Injector injector,
        Class<? extends AuthenticationContext> internalAuthContextClass) {
      this.cConf = cConf;
      this.injector = injector;
      this.internalAuthContextClass = internalAuthContextClass;
    }

    @Override
    public AuthenticationContext get() {
      if (SecurityUtil.isInternalAuthEnabled(cConf)) {
        return injector.getInstance(internalAuthContextClass);
      }
      return injector.getInstance(MasterAuthenticationContext.class);
    }
  }

  /**
   * A NO OP implementation for tests
   */
  private static final class NoOpAuditLogWriter implements AuditLogWriter {

    /**
     * pushes the log entry to respective messaging topic
     *
     * @param auditLogContexts
     */
    @Override
    public void publish(Queue<AuditLogContext> auditLogContexts) throws IOException {

    }
  }
}
