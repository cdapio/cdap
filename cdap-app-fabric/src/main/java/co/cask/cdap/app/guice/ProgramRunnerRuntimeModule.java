/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.data.stream.StreamWriter;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.stream.DefaultStreamWriter;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.internal.app.program.MessagingProgramStateWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;

/**
 *
 */
public final class ProgramRunnerRuntimeModule extends RuntimeModule {

  private final Class<? extends StreamWriter> streamWriterClass;

  public ProgramRunnerRuntimeModule() {
    this(DefaultStreamWriter.class);
  }

  @VisibleForTesting
  public ProgramRunnerRuntimeModule(Class<? extends StreamWriter> streamWriterClass) {
    this.streamWriterClass = streamWriterClass;
  }

  @Override
  public Module getInMemoryModules() {
    // No remote execution module in unit-test
    return Modules.combine(new InMemoryProgramRunnerModule(streamWriterClass),
                           new ProgramStateWriterModule());
  }

  @Override
  public Module getStandaloneModules() {
    // In standalone, we always use the DefaultStreamWriter
    return Modules.combine(new InMemoryProgramRunnerModule(DefaultStreamWriter.class),
                           new RemoteExecutionProgramRunnerModule(),
                           new ProgramStateWriterModule());
  }

  @Override
  public Module getDistributedModules() {
    return Modules.combine(new DistributedProgramRunnerModule(),
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
