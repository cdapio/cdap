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
package co.cask.cdap.common.guice;

import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;

/**
 * NamespaceClientRuntime binding for unit tests. These binding are similar to
 * {@link NamespaceClientRuntimeModule#getInMemoryModules()} but the {@link NamespaceQueryAdmin} is binded to a
 * {@link SimpleNamespaceQueryAdmin}. See documentation of {@link SimpleNamespaceQueryAdmin} for details.
 */
public class NamespaceClientUnitTestModule {
  public Module getModule() {

    return Modules.override(new NamespaceClientRuntimeModule().getInMemoryModules()).with(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(NamespaceQueryAdmin.class).to(SimpleNamespaceQueryAdmin.class);
        }
      }
    );
  }
}
