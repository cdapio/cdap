package com.continuuity.internal.app.runtime;

import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramOptions;

/**
 *
 */
public final class SimpleProgramOptions implements ProgramOptions {

  private final String name;
  private final Arguments arguments;
  private final Arguments userArguments;
  private final boolean debug;

  public SimpleProgramOptions(Program program) {
    this(program.getName(), new BasicArguments(), new BasicArguments());
  }

  public SimpleProgramOptions(String name, Arguments arguments, Arguments userArguments) {
    this(name, arguments, userArguments, false);
  }

  public SimpleProgramOptions(String name, Arguments arguments, Arguments userArguments, boolean debug) {
    this.name = name;
    this.arguments = arguments;
    this.userArguments = userArguments;
    this.debug = debug;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Arguments getArguments() {
    return arguments;
  }

  @Override
  public Arguments getUserArguments() {
    return userArguments;
  }

  @Override
  public boolean isDebug() {
    return debug;
  }
}
