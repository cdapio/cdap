/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.runtime;

import com.continuuity.app.program.Program;

/**
 *
 */
public interface ProgramRunner {

  /**
   * Runs the {@link Program} with the given {@link ProgramOptions}.
   * This method must returns immediately and have the {@link ProgramController} returned
   * state management.
   *
   * @param program
   * @param options
   * @return
   */
  ProgramController run(Program program, ProgramOptions options);
}
