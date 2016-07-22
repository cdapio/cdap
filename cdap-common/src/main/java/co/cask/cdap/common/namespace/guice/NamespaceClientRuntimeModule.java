/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.common.namespace.guice;

import co.cask.cdap.common.namespace.InMemoryNamespaceClient;
import co.cask.cdap.common.namespace.LocalNamespaceClient;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.RemoteNamespaceQueryClient;
import co.cask.cdap.common.runtime.RuntimeModule;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Singleton;

/**
 * Module to define Guice bindings for {@link NamespaceAdmin}.
 * {@link NamespaceAdmin} and {@link NamespaceQueryAdmin} are binded in Singleton to make sure
 * they use the same instance of the client.
 */
public class NamespaceClientRuntimeModule extends RuntimeModule {
  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        InMemoryNamespaceClient namespaceClient = new InMemoryNamespaceClient();
        bind(NamespaceAdmin.class).toInstance(namespaceClient);
        bind(NamespaceQueryAdmin.class).toInstance(namespaceClient);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LocalNamespaceClient.class).in(Singleton.class);
        bind(NamespaceAdmin.class).to(LocalNamespaceClient.class);
        bind(NamespaceQueryAdmin.class).to(LocalNamespaceClient.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(NamespaceQueryAdmin.class).to(RemoteNamespaceQueryClient.class);
      }
    };
  }
}
