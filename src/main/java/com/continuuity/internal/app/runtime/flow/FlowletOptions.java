/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.flow;

import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.RunId;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.google.common.collect.ImmutableMap;

/**
 *
 */
public final class FlowletOptions implements ProgramOptions {

  private final String name;
  private final Arguments arguments;
  private final Arguments userArguments;

  public FlowletOptions(String name, int instanceId, int instances, RunId runId) {
    this.name = name;
    this.arguments = new BasicArguments(
      ImmutableMap.of("instanceId", Integer.toString(instanceId),
                      "instances", Integer.toString(instances),
                      "runId", runId.getId()));
    this.userArguments = new BasicArguments();
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
}
