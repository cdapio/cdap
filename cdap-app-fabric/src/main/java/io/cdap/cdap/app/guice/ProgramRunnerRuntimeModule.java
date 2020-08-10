/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.runtime.RuntimeModule;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;

/**
 *
 */
public final class ProgramRunnerRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    // No remote execution module in unit-test
    return Modules.combine(new InMemoryProgramRunnerModule(),
                           new ProgramStateWriterModule());
  }

  @Override
  public Module getStandaloneModules() {
    return Modules.combine(new InMemoryProgramRunnerModule(),
                           new RemoteExecutionProgramRunnerModule(),
                           new ProgramStateWriterModule());
  }

  @Override
  public Module getDistributedModules() {
    return getDistributedModules(false);
  }

  /**
   * Creates a guice module for distributed mode.
   *
   * @param publishProgramState if {@code true}, program state will be published from the {@link ProgramRunner} upon
   *                            program state change. It only applies to {@link ProgramRunner} created for "native"
   *                            cluster execution. It doesn't applies to remote execution.
   */
  public Module getDistributedModules(boolean publishProgramState) {
    return Modules.combine(new DistributedProgramRunnerModule(publishProgramState),
                           new RemoteExecutionProgramRunnerModule(),
                           new ProgramStateWriterModule());
  }

  /**
   * Guice module for exposing the {@link ProgramStateWriter}.
   */
  private static final class ProgramStateWriterModule extends AbstractModule {

    @Override
    protected void configure() {
      // Bind ProgramStateWriter
      bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class).in(Scopes.SINGLETON);
    }
  }
}
