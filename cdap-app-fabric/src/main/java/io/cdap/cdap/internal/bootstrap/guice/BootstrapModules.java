/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.bootstrap.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.cdap.cdap.internal.bootstrap.BootstrapConfig;
import io.cdap.cdap.internal.bootstrap.BootstrapConfigProvider;
import io.cdap.cdap.internal.bootstrap.BootstrapService;
import io.cdap.cdap.internal.bootstrap.BootstrapStep;
import io.cdap.cdap.internal.bootstrap.FileBootstrapConfigProvider;
import io.cdap.cdap.internal.bootstrap.InMemoryBootstrapConfigProvider;
import io.cdap.cdap.internal.bootstrap.executor.AppCreator;
import io.cdap.cdap.internal.bootstrap.executor.BootstrapStepExecutor;
import io.cdap.cdap.internal.bootstrap.executor.DefaultNamespaceCreator;
import io.cdap.cdap.internal.bootstrap.executor.NativeProfileCreator;
import io.cdap.cdap.internal.bootstrap.executor.ProgramStarter;
import io.cdap.cdap.internal.bootstrap.executor.SystemArtifactLoader;
import io.cdap.cdap.internal.bootstrap.executor.SystemPreferenceSetter;
import io.cdap.cdap.internal.bootstrap.executor.SystemProfileCreator;

/**
 * Guice bindings for bootstrap classes. Binds {@link BootstrapService} as a singleton and binds
 * {@link BootstrapStep.Type} to a {@link BootstrapStepExecutor} responsible for executing that type of step.
 * For unit tests, the in memory module should be used, which will create the default namespace and native profile.
 * For actual CDAP instances, the file based module should be used.
 */
public class BootstrapModules {

  /**
   * @return bootstrap module for use in unit tests
   */
  public static Module getInMemoryModule() {
    return new BaseModule() {
      @Override
      protected void configure() {
        super.configure();
        BootstrapConfigProvider inMemoryProvider = new InMemoryBootstrapConfigProvider(BootstrapConfig.DEFAULT);
        bind(BootstrapConfigProvider.class).toInstance(inMemoryProvider);
      }
    };
  }

  /**
   * @return bootstrap module for use in sandbox and distributed
   */
  public static Module getFileBasedModule() {
    return new BaseModule() {
      @Override
      protected void configure() {
        super.configure();
        bind(BootstrapConfigProvider.class).to(FileBootstrapConfigProvider.class);
      }
    };
  }

  /**
   * Bindings common to all modules
   */
  private abstract static class BaseModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(BootstrapService.class).in(Scopes.SINGLETON);
      MapBinder<BootstrapStep.Type, BootstrapStepExecutor> mapBinder = MapBinder.newMapBinder(
        binder(), BootstrapStep.Type.class, BootstrapStepExecutor.class);
      mapBinder.addBinding(BootstrapStep.Type.CREATE_APPLICATION).to(AppCreator.class);
      mapBinder.addBinding(BootstrapStep.Type.CREATE_DEFAULT_NAMESPACE).to(DefaultNamespaceCreator.class);
      mapBinder.addBinding(BootstrapStep.Type.CREATE_NATIVE_PROFILE).to(NativeProfileCreator.class);
      mapBinder.addBinding(BootstrapStep.Type.CREATE_SYSTEM_PROFILE).to(SystemProfileCreator.class);
      mapBinder.addBinding(BootstrapStep.Type.LOAD_SYSTEM_ARTIFACTS).to(SystemArtifactLoader.class);
      mapBinder.addBinding(BootstrapStep.Type.SET_SYSTEM_PROPERTIES).to(SystemPreferenceSetter.class);
      mapBinder.addBinding(BootstrapStep.Type.START_PROGRAM).to(ProgramStarter.class);
    }
  }
}
