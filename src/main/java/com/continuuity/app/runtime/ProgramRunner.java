package com.continuuity.app.runtime;

import com.continuuity.app.program.Program;

/**
 *
 */
public interface ProgramRunner {

  Controller run(Program program, ProgramOptions options);
}
