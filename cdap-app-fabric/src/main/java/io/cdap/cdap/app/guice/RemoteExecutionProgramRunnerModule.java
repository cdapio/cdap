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
 */

package io.cdap.cdap.app.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedMapReduceProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkerProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkflowProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import io.cdap.cdap.internal.app.services.ProgramCompletionNotifier;
import io.cdap.cdap.proto.ProgramType;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;

/**
 * Guice module for {@link ProgramRunnerFactory} used for program execution in {@link ClusterMode#ISOLATED} mode.
 */
final class RemoteExecutionProgramRunnerModule extends AbstractModule {

  private static final Key<TwillRunnerService> TWILL_RUNNER_SERVICE_KEY =
    Key.get(TwillRunnerService.class, Constants.AppFabric.RemoteExecution.class);

  @Override
  protected void configure() {
    install(createPrivateModule());

    // Creates a multibinding to the twill runner services for capturing program state changes.
    // It has to be public binding because this set is injected to program notification subscriber.
    Multibinder<ProgramCompletionNotifier> multiBinder = Multibinder.newSetBinder(binder(),
                                                                                  ProgramCompletionNotifier.class);
    multiBinder.addBinding().toProvider(ProgramCompletionNotifierProvider.class);
  }

  /**
   * Creates the private module for bindings used for remote program execution.
   */
  private Module createPrivateModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        // Bind the TwillRunner for remote execution used in isolated cluster.
        // The binding is added in here instead of in TwillModule is because this module can be used
        // in standalone env as well and it doesn't require YARN.
        bind(TWILL_RUNNER_SERVICE_KEY).to(RemoteExecutionTwillRunnerService.class).in(Scopes.SINGLETON);
        expose(TWILL_RUNNER_SERVICE_KEY);

        // Bind ProgramRunnerFactory and expose it with the RemoteExecution annotation
        Key<ProgramRunnerFactory> programRunnerFactoryKey = Key.get(ProgramRunnerFactory.class,
                                                                    Constants.AppFabric.RemoteExecution.class);
        // ProgramRunnerFactory should be in distributed mode
        bind(ProgramRuntimeProvider.Mode.class).toInstance(ProgramRuntimeProvider.Mode.DISTRIBUTED);
        bind(programRunnerFactoryKey).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);
        expose(programRunnerFactoryKey);

        // The following are bindings are for ProgramRunners. They are private to this module and only
        // available to the remote execution ProgramRunnerFactory exposed.

        // This set of program runners are for isolated mode
        bind(ClusterMode.class).toInstance(ClusterMode.ISOLATED);
        // TwillRunner used by the ProgramRunner is the remote execution one
        bind(TwillRunner.class).annotatedWith(Constants.AppFabric.ProgramRunner.class).to(TWILL_RUNNER_SERVICE_KEY);
        // ProgramRunnerFactory used by ProgramRunner is the remote execution one.
        bind(ProgramRunnerFactory.class)
          .annotatedWith(Constants.AppFabric.ProgramRunner.class)
          .to(programRunnerFactoryKey);

        // A private Map binding of ProgramRunner for ProgramRunnerFactory to use
        MapBinder<ProgramType, ProgramRunner> defaultProgramRunnerBinder = MapBinder.newMapBinder(
          binder(), ProgramType.class, ProgramRunner.class);

        defaultProgramRunnerBinder.addBinding(ProgramType.MAPREDUCE).to(DistributedMapReduceProgramRunner.class);
        defaultProgramRunnerBinder.addBinding(ProgramType.WORKFLOW).to(DistributedWorkflowProgramRunner.class);
        defaultProgramRunnerBinder.addBinding(ProgramType.WORKER).to(DistributedWorkerProgramRunner.class);
      }
    };
  }

  /**
   * Provider for {@link ProgramCompletionNotifier}, which is optionally implemented by {@link TwillRunnerService}.
   */
  private static final class ProgramCompletionNotifierProvider implements Provider<ProgramCompletionNotifier> {

    private final Injector injector;

    @Inject
    ProgramCompletionNotifierProvider(Injector injector) {
      this.injector = injector;
    }

    @Override
    public ProgramCompletionNotifier get() {
      TwillRunnerService twillRunner = injector.getInstance(TWILL_RUNNER_SERVICE_KEY);
      if (twillRunner instanceof ProgramCompletionNotifier) {
        return (ProgramCompletionNotifier) twillRunner;
      }
      // This shouldn't be hit, but to safeguard.
      return (programRunId, completionStatus) -> { };
    }
  }
}
