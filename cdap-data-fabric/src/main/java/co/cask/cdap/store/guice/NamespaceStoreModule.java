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

package co.cask.cdap.store.guice;

import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.store.DefaultNamespaceStore;
import co.cask.cdap.store.InMemoryNamespaceStore;
import co.cask.cdap.store.NamespaceStore;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;


/**
 * Module to define Guice bindings for {@link NamespaceStore}
 */
public class NamespaceStoreModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(NamespaceStore.class).to(InMemoryNamespaceStore.class).in(Scopes.SINGLETON);
        expose(NamespaceStore.class);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(NamespaceStore.class).to(DefaultNamespaceStore.class);
        expose(NamespaceStore.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(NamespaceStore.class).to(DefaultNamespaceStore.class);
        expose(NamespaceStore.class);
      }
    };
  }
}
