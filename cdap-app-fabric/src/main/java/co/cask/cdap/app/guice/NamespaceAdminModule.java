/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.guice;

import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.internal.app.namespace.DefaultNamespaceAdmin;
import co.cask.cdap.internal.app.namespace.DistributedNamespaceResourceDeleter;
import co.cask.cdap.internal.app.namespace.LocalNamespaceResourceDeleter;
import co.cask.cdap.internal.app.namespace.NamespaceResourceDeleter;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

/**
 * Namespace admin modules
 */
public class NamespaceAdminModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(NamespaceResourceDeleter.class).to(LocalNamespaceResourceDeleter.class).in(Scopes.SINGLETON);
        bind(DefaultNamespaceAdmin.class).in(Scopes.SINGLETON);
        bind(NamespaceAdmin.class).to(DefaultNamespaceAdmin.class);
        bind(NamespaceQueryAdmin.class).to(DefaultNamespaceAdmin.class);

        expose(NamespaceAdmin.class);
        expose(NamespaceQueryAdmin.class);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(NamespaceResourceDeleter.class).to(LocalNamespaceResourceDeleter.class).in(Scopes.SINGLETON);
        bind(DefaultNamespaceAdmin.class).in(Scopes.SINGLETON);
        bind(NamespaceAdmin.class).to(DefaultNamespaceAdmin.class);
        bind(NamespaceQueryAdmin.class).to(DefaultNamespaceAdmin.class);

        expose(NamespaceAdmin.class);
        expose(NamespaceQueryAdmin.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(NamespaceResourceDeleter.class).to(DistributedNamespaceResourceDeleter.class).in(Scopes.SINGLETON);
        bind(DefaultNamespaceAdmin.class).in(Scopes.SINGLETON);
        bind(NamespaceAdmin.class).to(DefaultNamespaceAdmin.class);
        bind(NamespaceQueryAdmin.class).to(DefaultNamespaceAdmin.class);

        expose(NamespaceAdmin.class);
        expose(NamespaceQueryAdmin.class);
      }
    };
  }
}
