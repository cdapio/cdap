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

package co.cask.cdap.security.authorization;

import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.PrivilegesFetcher;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

import java.util.Set;

/**
 * A module that contains bindings for {@link AuthorizationEnforcementService} and {@link PrivilegesFetcher}.
 */
public class AuthorizationEnforcementModule extends RuntimeModule {
  public static final String PRIVILEGES_FETCHER_PROXY_CACHE = "privileges-fetcher-proxy-cache";
  public static final String PRIVILEGES_FETCHER_PROXY = "privileges-fetcher-proxy";

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        // bind AuthorizationEnforcementService as a singleton. This binding is used while starting/stopping
        // the service itself.
        bind(AuthorizationEnforcementService.class).to(DefaultAuthorizationEnforcementService.class)
          .in(Scopes.SINGLETON);
        // bind AuthorizationEnforcer to AuthorizationEnforcementService
        bind(AuthorizationEnforcer.class).to(AuthorizationEnforcementService.class).in(Scopes.SINGLETON);

        bind(PrivilegesFetcherProxyService.class).to(DefaultPrivilegesFetcherProxyService.class)
          .in(Scopes.SINGLETON);
        bind(PrivilegesFetcher.class).to(AuthorizerAsPrivilegesFetcher.class).in(Scopes.SINGLETON);
        bind(PrivilegesFetcher.class)
          .annotatedWith(Names.named(PRIVILEGES_FETCHER_PROXY_CACHE))
          .to(PrivilegesFetcherProxyService.class);
        bind(PrivilegesFetcher.class)
          .annotatedWith(Names.named(PRIVILEGES_FETCHER_PROXY))
          .to(AuthorizerAsPrivilegesFetcher.class);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        // bind AuthorizationEnforcementService as a singleton. This binding is used while starting/stopping
        // the service itself.
        bind(AuthorizationEnforcementService.class).to(DefaultAuthorizationEnforcementService.class)
          .in(Scopes.SINGLETON);
        // bind AuthorizationEnforcer to AuthorizationEnforcementService
        bind(AuthorizationEnforcer.class).to(AuthorizationEnforcementService.class).in(Scopes.SINGLETON);

        bind(PrivilegesFetcherProxyService.class).to(DefaultPrivilegesFetcherProxyService.class)
          .in(Scopes.SINGLETON);
        bind(PrivilegesFetcher.class).to(AuthorizerAsPrivilegesFetcher.class).in(Scopes.SINGLETON);
        bind(PrivilegesFetcher.class)
          .annotatedWith(Names.named(PRIVILEGES_FETCHER_PROXY_CACHE))
          .to(PrivilegesFetcherProxyService.class);
        bind(PrivilegesFetcher.class)
          .annotatedWith(Names.named(PRIVILEGES_FETCHER_PROXY))
          .to(AuthorizerAsPrivilegesFetcher.class);
      }
    };
  }

  /**
   * Used by program containers and system services (viz explore service, stream service) that need to enforce
   * authorization in distributed mode. For fetching privileges, these components are expected to proxy via a proxy
   * service, which in turn uses the authorization enforcement modules defined by #getProxyModule
   */
  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        // bind AuthorizationEnforcementService as a singleton. This binding is used while starting/stopping
        // the service itself.
        bind(AuthorizationEnforcementService.class).to(DefaultAuthorizationEnforcementService.class)
          .in(Scopes.SINGLETON);
        // bind AuthorizationEnforcer to AuthorizationEnforcementService
        bind(AuthorizationEnforcer.class).to(AuthorizationEnforcementService.class).in(Scopes.SINGLETON);
        bind(PrivilegesFetcher.class).to(RemotePrivilegesFetcher.class);
      }
    };
  }

  /**
   * Returns an {@link AbstractModule} containing bindings for authorization enforcement to be used in the Master.
   */
  public AbstractModule getMasterModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        // bind AuthorizationEnforcementService as a singleton. This binding is used while starting/stopping
        // the service itself.
        bind(AuthorizationEnforcementService.class).to(DefaultAuthorizationEnforcementService.class)
          .in(Scopes.SINGLETON);
        // bind AuthorizationEnforcer to AuthorizationEnforcementService
        bind(AuthorizationEnforcer.class).to(AuthorizationEnforcementService.class).in(Scopes.SINGLETON);

        // Master should have access to authorization backends, so no need to fetch privileges remotely
        bind(PrivilegesFetcher.class).to(AuthorizerAsPrivilegesFetcher.class);

        // Master runs a proxy caching service for privileges for system services and program containers to fetch
        // privileges from authorization backends.
        // The Master service acts as a proxy for system services and program containers to authorization backends
        // for fetching privileges, since they may not have access to make requests to authorization backends.
        // e.g. Apache Sentry currently does not support proxy authentication or issue delegation tokens. As a result,
        // all requests to Sentry need to be proxied via Master, which is whitelisted.
        // Hence, bind PrivilegesFetcher to a proxy implementation, that makes a proxy call to master for fetching
        // privileges
        // bind PrivilegesFetcherProxyService as a singleton. This binding is used while starting/stopping
        // the service itself.
        bind(PrivilegesFetcherProxyService.class).to(DefaultPrivilegesFetcherProxyService.class)
          .in(Scopes.SINGLETON);
        bind(PrivilegesFetcher.class)
          .annotatedWith(Names.named(PRIVILEGES_FETCHER_PROXY_CACHE))
          .to(PrivilegesFetcherProxyService.class);
        // Master is expected to have (kerberos) credentials to communicate with authorization backends. Hence, bind
        // PrivilegesFetcher to the configured Authorizer
        bind(PrivilegesFetcher.class)
          .annotatedWith(Names.named(PRIVILEGES_FETCHER_PROXY))
          .to(AuthorizerAsPrivilegesFetcher.class);

      }
    };
  }

  /**
   * Provides {@link Authorizer} as the binding for {@link PrivilegesFetcher}.
   */
  private static class AuthorizerAsPrivilegesFetcher implements PrivilegesFetcher {
    private final AuthorizerInstantiator authorizerInstantiator;

    @Inject
    private AuthorizerAsPrivilegesFetcher(AuthorizerInstantiator authorizerInstantiator) {
      this.authorizerInstantiator = authorizerInstantiator;
    }

    @Override
    public Set<Privilege> listPrivileges(Principal principal) throws Exception {
      return authorizerInstantiator.get().listPrivileges(principal);
    }
  }
}
