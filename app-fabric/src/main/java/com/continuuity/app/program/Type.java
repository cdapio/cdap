/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.internal.app.runtime.webapp.WebappSpecification;

/**
 * Defines types of programs supported by the system.
 */
public enum Type {
  FLOW(1, "Flow", FlowSpecification.class),
  PROCEDURE(2, "Procedure", ProcedureSpecification.class),
  MAPREDUCE(3, "Mapreduce", MapReduceSpecification.class),
  WORKFLOW(4, "Workflow", WorkflowSpecification.class),
  WEBAPP(5, "WebApp", WebappSpecification.class),
  SERVICE(6, "Service", ServiceSpecification.class);

  private final int programType;
  private final String prettyName;
  private final Class<? extends ProgramSpecification> specClass;

  private Type(int type, String prettyName, Class<? extends ProgramSpecification> specClass) {
    this.programType = type;
    this.prettyName = prettyName;
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

  public String prettyName() {
    return prettyName;
  }

  public static Type valueOfPrettyName(String pretty) {
    return valueOf(pretty.toUpperCase());
  }

}
