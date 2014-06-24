/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime;

import com.continuuity.app.runtime.ProgramRunner;

/**
 * Factory for creating {@link ProgramRunner}.
 */
public interface ProgramRunnerFactory {

  /**
   * Types of program that could be created.
   */
  public enum Type {
    FLOW,
    FLOWLET,
    PROCEDURE,
    MAPREDUCE,
    WORKFLOW,
    WEBAPP,
    SERVICE,
    RUNNABLE
  }

  ProgramRunner create(Type programType);
}
