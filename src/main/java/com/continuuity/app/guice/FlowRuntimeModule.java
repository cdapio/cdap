package com.continuuity.app.guice;

import com.continuuity.api.io.SchemaGenerator;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.internal.app.runtime.FlowProgramRunner;
import com.continuuity.internal.app.runtime.FlowletProgramRunner;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;

/**
 *
 */
public class FlowRuntimeModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ProgramRunnerFactory.class).to(InMemoryProgramRunnerFactory.class);
    bind(SchemaGenerator.class).to(ReflectionSchemaGenerator.class);
    bind(FlowProgramRunner.class);
  }


  // TODO Temporary
  @Singleton
  private static final class InMemoryProgramRunnerFactory implements ProgramRunnerFactory {

    private final Injector injector;

    @Inject
    private InMemoryProgramRunnerFactory(Injector injector) {
      this.injector = injector;
    }

    @Override
    public ProgramRunner create() {
      return injector.getInstance(FlowletProgramRunner.class);
    }
  }
}
