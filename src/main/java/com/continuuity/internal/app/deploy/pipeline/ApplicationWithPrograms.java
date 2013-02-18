/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.app.program.Program;
import com.google.common.collect.ImmutableList;

/**
 *
 */
public final class ApplicationWithPrograms {
  private final ApplicationSpecLocation appSpecLoc;
  private final ImmutableList<Program> programs;

  public ApplicationWithPrograms(ApplicationSpecLocation appSpecLoc, ImmutableList<Program> programs) {
    this.appSpecLoc = appSpecLoc;
    this.programs = programs;
  }

  public ApplicationSpecLocation getAppSpecLoc() {
    return appSpecLoc;
  }

  public Iterable<Program> getPrograms() {
    return programs;
  }
}
