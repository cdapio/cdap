/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;

/**
 * Defines types of programs supported by the system.
 */
public enum Type {
  FLOW(1, FlowSpecification.class),
  PROCEDURE(2, ProcedureSpecification.class),
  MAPREDUCE(3, MapReduceSpecification.class),
  WORKFLOW(4, WorkflowSpecification.class),
  WEBAPP(5, ProgramSpecification.class);

  private final int programType;
  private final Class<? extends ProgramSpecification> specClass;

  private Type(int type, Class<? extends ProgramSpecification> specClass) {
    this.programType = type;
    this.specClass = specClass;
  }

  public static Type typeOfSpecification(ProgramSpecification spec) {
    Class<? extends ProgramSpecification> specClass = spec.getClass();
    for (Type type : Type.values()) {
      if (type.specClass.isAssignableFrom(specClass)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown specification type: " + specClass);
  }
}
