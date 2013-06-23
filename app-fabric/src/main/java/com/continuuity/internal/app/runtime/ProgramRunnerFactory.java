/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime;

import com.continuuity.app.runtime.ProgramRunner;

/**
 *
 */
public interface ProgramRunnerFactory {

  public enum Type {
    FLOW,
    FLOWLET,
    PROCEDURE,
    MAPREDUCE
  }

  ProgramRunner create(Type programType);
}
