/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.cdap.cdap.common.runtime.RuntimeModule;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.security.spi.authorization.Authorizer;

/**
 * A module that contains bindings for {@link AuthorizationEnforcer}.
 */
public class AuthorizationEnforcementModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(AccessEnforcer.class).to(DefaultAccessEnforcer.class).in(Scopes.SINGLETON);
        bind(AuthorizationEnforcer.class).to(AccessEnforcerWrapper.class).in(Scopes.SINGLETON);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(AccessEnforcer.class).to(DefaultAccessEnforcer.class).in(Scopes.SINGLETON);
        bind(AuthorizationEnforcer.class).to(AccessEnforcerWrapper.class).in(Scopes.SINGLETON);
      }
    };
  }

  /**
   * Used by program containers and system services (viz explore service, stream service) that need to enforce
   * authorization in distributed mode.
   */
  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(AccessEnforcer.class).to(RemoteAccessEnforcer.class).in(Scopes.SINGLETON);
        bind(AuthorizationEnforcer.class).to(AccessEnforcerWrapper.class).in(Scopes.SINGLETON);
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
        bind(AccessEnforcer.class).to(DefaultAccessEnforcer.class).in(Scopes.SINGLETON);
        bind(AuthorizationEnforcer.class).to(AccessEnforcerWrapper.class).in(Scopes.SINGLETON);
      }
    };
  }
}
