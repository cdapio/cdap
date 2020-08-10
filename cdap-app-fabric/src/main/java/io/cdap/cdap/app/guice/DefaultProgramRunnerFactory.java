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

package io.cdap.cdap.app.guice;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramClassLoaderProvider;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramControllerCreator;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.internal.app.program.StateChangeListener;
import io.cdap.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The default implementation of {@link ProgramRunnerFactory} used inside app-fabric.
 */
public final class DefaultProgramRunnerFactory implements ProgramRunnerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultProgramRunnerFactory.class);

  private final Injector injector;
  private final Map<ProgramType, Provider<ProgramRunner>> defaultRunnerProviders;
  private final ProgramRuntimeProvider.Mode mode;
  private final ProgramRuntimeProviderLoader runtimeProviderLoader;
  private final ProgramStateWriter programStateWriter;
  private boolean publishProgramState;

  @Inject
  DefaultProgramRunnerFactory(Injector injector, ProgramRuntimeProvider.Mode mode,
                              ProgramRuntimeProviderLoader runtimeProviderLoader,
                              Map<ProgramType, Provider<ProgramRunner>> defaultRunnerProviders,
                              ProgramStateWriter programStateWriter) {
    this.injector = injector;
    this.defaultRunnerProviders = defaultRunnerProviders;
    this.mode = mode;
    this.runtimeProviderLoader = runtimeProviderLoader;
    this.programStateWriter = programStateWriter;
  }

  @Inject(optional = true)
  void setPublishProgramState(@Named("publishProgramState") boolean publishProgramState) {
    this.publishProgramState = publishProgramState;
  }

  @Override
  public ProgramRunner create(ProgramType programType) {
    ProgramRuntimeProvider provider = runtimeProviderLoader.get(programType);
    ProgramRunner runner;

    if (provider != null) {
      LOG.trace("Using runtime provider {} for program type {}", provider, programType);
      runner = provider.createProgramRunner(programType, mode, injector);
    } else {
      Provider<ProgramRunner> defaultProvider = defaultRunnerProviders.get(programType);
      if (defaultProvider == null) {
        throw new IllegalArgumentException("Unsupported program type: " + programType);
      }
      runner = defaultProvider.get();
    }

    // Wrap a state change listener around the controller returned by the program runner in standalone mode
    // In distributed mode, program state changes are recorded in the event handler by Twill AM.
    return (mode == ProgramRuntimeProvider.Mode.LOCAL || publishProgramState) ? wrapProgramRunner(runner) : runner;
  }

  /**
   * Wraps the given {@link ProgramRunner} with program state publishing.
   */
  private ProgramRunner wrapProgramRunner(ProgramRunner programRunner) {
    if (programRunner instanceof ProgramControllerCreator) {
      return new StatePublishProgramRunnerControllerCreator(programRunner, programStateWriter);
    }
    return new StatePublishProgramRunner(programRunner, programStateWriter);
  }

  /**
   * A {@link ProgramRunner} that wraps around another {@link ProgramRunner} and publish events on program state change.
   */
  private static class StatePublishProgramRunner implements ProgramRunner, ProgramClassLoaderProvider, Closeable {

    private final ProgramRunner runner;
    private final ProgramStateWriter programStateWriter;

    private StatePublishProgramRunner(ProgramRunner runner, ProgramStateWriter programStateWriter) {
      this.runner = runner;
      this.programStateWriter = programStateWriter;
    }

    @Override
    public ProgramController run(Program program, ProgramOptions options) {
      return addStateChangeListener(runner.run(program, options));
    }

    protected ProgramController addStateChangeListener(ProgramController controller) {
      controller.addListener(new StateChangeListener(controller, programStateWriter), Threads.SAME_THREAD_EXECUTOR);
      return controller;
    }

    @Override
    @Nullable
    public ClassLoader createProgramClassLoaderParent() {
      if (runner instanceof ProgramClassLoaderProvider) {
        return ((ProgramClassLoaderProvider) runner).createProgramClassLoaderParent();
      }
      return null;
    }

    @Override
    public void close() throws IOException {
      if (runner instanceof Closeable) {
        ((Closeable) runner).close();
      }
    }
  }

  /**
   * A {@link StatePublishProgramRunner} that also implements {@link ProgramControllerCreator}.
   * It is implemented by delegating to the underlying {@link ProgramRunner}
   * that also implement {@link ProgramControllerCreator}.
   */
  private static class StatePublishProgramRunnerControllerCreator
    extends StatePublishProgramRunner implements ProgramControllerCreator {

    private final ProgramControllerCreator controllerCreator;

    private StatePublishProgramRunnerControllerCreator(ProgramRunner runner, ProgramStateWriter programStateWriter) {
      super(runner, programStateWriter);
      if (!(runner instanceof ProgramControllerCreator)) {
        throw new IllegalArgumentException("ProgramRunner " + runner +
                                             " does not implement " + ProgramControllerCreator.class);
      }
      this.controllerCreator = (ProgramControllerCreator) runner;
    }

    @Override
    public ProgramController createProgramController(TwillController twillController,
                                                     ProgramId programId, RunId runId) {
      return addStateChangeListener(controllerCreator.createProgramController(twillController, programId, runId));
    }
  }
}
