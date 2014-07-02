/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.internal.app.ForwardingApplicationSpecification;
import com.continuuity.internal.app.program.ForwardingProgram;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * A Forwarding Program that turns a Workflow Program into a MapReduce program.
 */
public final class WorkflowMapReduceProgram extends ForwardingProgram {

  private final MapReduceSpecification mapReduceSpec;

  public WorkflowMapReduceProgram(Program delegate, MapReduceSpecification mapReduceSpec) {
    super(delegate);
    this.mapReduceSpec = mapReduceSpec;
  }

  @Override
  public String getMainClassName() {
    return mapReduceSpec.getClassName();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Class<T> getMainClass() throws ClassNotFoundException {
    return (Class<T>) Class.forName(getMainClassName(), true, getClassLoader());
  }

  @Override
  public Type getType() {
    return Type.MAPREDUCE;
  }

  @Override
  public Id.Program getId() {
    return Id.Program.from(getAccountId(), getApplicationId(), getName());
  }

  @Override
  public String getName() {
    return mapReduceSpec.getName();
  }

  @Override
  public ApplicationSpecification getSpecification() {
    return new ForwardingApplicationSpecification(super.getSpecification()) {
      @Override
      public Map<String, MapReduceSpecification> getMapReduce() {
        return ImmutableMap.of(mapReduceSpec.getName(), mapReduceSpec);
      }
    };
  }
}
