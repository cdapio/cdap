/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
