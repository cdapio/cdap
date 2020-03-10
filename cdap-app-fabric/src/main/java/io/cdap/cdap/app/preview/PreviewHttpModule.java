/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.preview;

import com.google.inject.Binder;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.gateway.handlers.preview.PreviewHttpHandler;
import io.cdap.cdap.internal.app.preview.DefaultPreviewManager;

/**
 * Provides bindings required create the {@link PreviewHttpHandler}.
 */
public class PreviewHttpModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(DatasetDefinitionRegistryFactory.class)
      .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

    bind(DatasetFramework.class)
      .annotatedWith(Names.named(DataSetsModules.BASE_DATASET_FRAMEWORK))
      .to(RemoteDatasetFramework.class);
    bindPreviewRunnerFactory(binder());
    bind(PreviewManager.class).to(DefaultPreviewManager.class).in(Scopes.SINGLETON);
    expose(PreviewManager.class);
  }

  /**
   * Binds an implementation for {@link PreviewRunnerModuleFactory}.
   */
  protected void bindPreviewRunnerFactory(Binder binder) {
    binder.install(
      new FactoryModuleBuilder()
        .implement(PreviewRunnerModule.class, DefaultPreviewRunnerModule.class)
        .build(PreviewRunnerModuleFactory.class)
    );
  }
}
