package com.continuuity.internal.app.runtime;

import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramOptions;

public class DefaultProgramOptions implements ProgramOptions {
  private final Program program;

  public DefaultProgramOptions(Program program) {
    this.program = program;
  }

  @Override
  public String getName() {
    return program.getProgramName();
  }

  @Override
  public Arguments getArguments() {
    return new BasicArguments();
  }

  @Override
  public Arguments getUserArguments() {
    return new BasicArguments();
  }
}
