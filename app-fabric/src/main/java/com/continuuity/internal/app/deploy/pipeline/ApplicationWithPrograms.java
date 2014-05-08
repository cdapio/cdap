/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.app.program.Program;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *
 */
public final class ApplicationWithPrograms {
  private final ApplicationSpecLocation appSpecLoc;
  private final List<Program> programs;

  public ApplicationWithPrograms(ApplicationSpecLocation appSpecLoc, List<? extends Program> programs) {
    this.appSpecLoc = appSpecLoc;
    this.programs = ImmutableList.copyOf(programs);
  }

  public ApplicationSpecLocation getAppSpecLoc() {
    return appSpecLoc;
  }

  public Iterable<Program> getPrograms() {
    return programs;
  }
}
