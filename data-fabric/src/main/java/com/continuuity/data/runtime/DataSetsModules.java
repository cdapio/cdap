/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data.runtime;

import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.data2.datafabric.dataset.RemoteDatasetFramework;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeClassLoaderFactory;
import com.continuuity.data2.datafabric.dataset.type.DistributedDatasetTypeClassLoaderFactory;
import com.continuuity.data2.datafabric.dataset.type.LocalDatasetTypeClassLoaderFactory;
import com.continuuity.data2.dataset2.DatasetDefinitionRegistryFactory;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DefaultDatasetDefinitionRegistry;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

/**
 * DataSets framework bindings
 */
public class DataSetsModules {
  public Module getInMemoryModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));
        bind(DatasetTypeClassLoaderFactory.class).to(LocalDatasetTypeClassLoaderFactory.class);
        expose(DatasetTypeClassLoaderFactory.class);
        bind(DatasetFramework.class).to(RemoteDatasetFramework.class);
        expose(DatasetFramework.class);
      }
    };

  }

  public Module getLocalModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));
        bind(DatasetTypeClassLoaderFactory.class).to(LocalDatasetTypeClassLoaderFactory.class);
        expose(DatasetTypeClassLoaderFactory.class);
        bind(DatasetFramework.class).to(RemoteDatasetFramework.class);
        expose(DatasetFramework.class);
      }
    };

  }

  public Module getDistributedModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));
        bind(DatasetTypeClassLoaderFactory.class).to(DistributedDatasetTypeClassLoaderFactory.class);
        expose(DatasetTypeClassLoaderFactory.class);
        bind(DatasetFramework.class).to(RemoteDatasetFramework.class);
        expose(DatasetFramework.class);
      }
    };
  }
}
