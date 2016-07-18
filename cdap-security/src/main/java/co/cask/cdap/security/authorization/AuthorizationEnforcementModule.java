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
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesFetcher;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;

/**
 * A module that contains bindings for {@link AuthorizationEnforcementService} and {@link PrivilegesFetcher}.
 */
public class AuthorizationEnforcementModule extends RuntimeModule {

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
        bind(PrivilegesFetcher.class).toProvider(InMemoryPrivilegesFetcherProvider.class).in(Scopes.SINGLETON);
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
        bind(PrivilegesFetcher.class).to(RemotePrivilegesFetcher.class);
      }
    };
  }

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

  private static class InMemoryPrivilegesFetcherProvider implements Provider<PrivilegesFetcher> {
    private final AuthorizerInstantiator authorizerInstantiator;

    @Inject
    private InMemoryPrivilegesFetcherProvider(AuthorizerInstantiator authorizerInstantiator) {
      this.authorizerInstantiator = authorizerInstantiator;
    }

    @Override
    public PrivilegesFetcher get() {
      return authorizerInstantiator.get();
    }
  }
}
