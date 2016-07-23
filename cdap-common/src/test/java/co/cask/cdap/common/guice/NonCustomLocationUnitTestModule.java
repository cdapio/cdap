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

import co.cask.cdap.common.namespace.DefaultNamespacedLocationFactory;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.NamespacedLocationFactoryTestClient;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;

/**
 * Location Factory guice binding for unit tests. These binding are similar to
 * {@link LocationRuntimeModule#getInMemoryModules()} but the {@link NamespacedLocationFactory} is binded to a
 * {@link NamespacedLocationFactoryTestClient} which does not perform {@link NamespaceMeta} lookup like
 * {@link DefaultNamespacedLocationFactory} and hence in unit tests the namespace does not need to be created to get
 * namespaces locations.
 */
public class NonCustomLocationUnitTestModule {
  public Module getModule() {

    return Modules.override(new LocationRuntimeModule().getInMemoryModules()).with(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(NamespacedLocationFactory.class).to(NamespacedLocationFactoryTestClient.class);
        }
      }
    );
  }
}
