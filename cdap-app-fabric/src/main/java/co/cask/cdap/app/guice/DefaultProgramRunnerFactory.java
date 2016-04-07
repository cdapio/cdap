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

package co.cask.cdap.app.guice;

import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.app.runtime.ProgramRuntimeProvider;
import co.cask.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The default implementation of {@link ProgramRunnerFactory} used inside app-fabric.
 */
public final class DefaultProgramRunnerFactory implements ProgramRunnerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultProgramRunnerFactory.class);

  private final Injector injector;
  private final Map<ProgramType, Provider<ProgramRunner>> defaultRunnerProviders;
  private final ProgramRuntimeProvider.Mode mode;
  private final ProgramRuntimeProviderLoader runtimeProviderLoader;


  @Inject
  DefaultProgramRunnerFactory(Injector injector, ProgramRuntimeProvider.Mode mode,
                              ProgramRuntimeProviderLoader runtimeProviderLoader,
                              Map<ProgramType, Provider<ProgramRunner>> defaultRunnerProviders) {
    this.injector = injector;
    this.defaultRunnerProviders = defaultRunnerProviders;
    this.mode = mode;
    this.runtimeProviderLoader = runtimeProviderLoader;
  }

  @Override
  public ProgramRunner create(ProgramType programType) {
    ProgramRuntimeProvider provider = runtimeProviderLoader.get(programType);
    if (provider != null) {
      LOG.debug("Using runtime provider {} for program type {}", provider, programType);
      return provider.createProgramRunner(programType, mode, injector);
    }

    Provider<ProgramRunner> defaultProvider = defaultRunnerProviders.get(programType);
    Preconditions.checkNotNull(defaultProvider, "Unsupported program type: " + programType);
    return defaultProvider.get();
  }
}
