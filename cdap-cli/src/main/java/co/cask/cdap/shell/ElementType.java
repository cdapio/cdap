/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.shell;

import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Represents types of programs and their elements.
 */
public enum ElementType {

  APP("application", "applications", "app", "apps", null, null),
  DATASET("Dataset", "Datasets", "dataset", "datasets", null, null),
  DATASET_MODULE("Dataset module", "Dataset modules", "dataset module", "dataset modules", null, null),
  DATASET_TYPE("Dataset type", "Dataset types", "dataset type", "dataset types", null, null),
  QUERY("Dataset query", "Dataset queries", "dataset query", "dataset queries", null, null),
  STREAM("Stream", "Streams", "stream", "streams", null, null),
  PROGRAM("program", "programs", "program", "programs", null, null),

  FLOW("Flow", "Flows", "flow", "flows", ProgramType.FLOW, null,
       Capability.HISTORY, Capability.LOGS, Capability.LIVE_INFO, Capability.STATUS, Capability.START_STOP),

  WORKFLOW("Workflow", "Workflows", "workflow", "workflows", ProgramType.WORKFLOW, null,
           Capability.HISTORY, Capability.STATUS, Capability.START_STOP),

  FLOWLET("Flowlet", "Flowlets", "flowlet", "flowlets", null, ProgramType.FLOW,
          Capability.SCALE),

  PROCEDURE("Procedure", "Procedures", "procedure", "procedures", ProgramType.PROCEDURE, null,
            Capability.HISTORY, Capability.SCALE, Capability.LOGS, Capability.LIVE_INFO, Capability.STATUS,
            Capability.START_STOP),

  SERVICE("Service", "Services", "service", "services", ProgramType.SERVICE, null,
          Capability.START_STOP, Capability.STATUS),

  RUNNABLE("Runnable", "Runnables", "runnable", "runnables", null, ProgramType.SERVICE,
           Capability.SCALE, Capability.HISTORY, Capability.LOGS),

  MAPREDUCE("MapReduce job", "MapReduce jobs", "mapreduce", "mapreduce", ProgramType.MAPREDUCE, null,
            Capability.LOGS, Capability.HISTORY, Capability.STATUS, Capability.START_STOP);

  private final String pluralName;
  private final String pluralPrettyName;
  private final String name;
  private final ProgramType programType;
  private final ProgramType parentType;
  private final Set<Capability> capabilities;
  private final String prettyName;

  ElementType(String prettyName, String pluralPrettyName,
              String name, String pluralName,
              ProgramType programType, ProgramType parentType,
              Capability... capabilities) {
    this.prettyName = prettyName;
    this.pluralPrettyName = pluralPrettyName;
    this.name = name;
    this.pluralName = pluralName;
    this.programType = programType;
    this.parentType = parentType;
    this.capabilities = Sets.newHashSet(capabilities);
  }

  public boolean isTopLevel() {
    return parentType == null;
  }

  public String getPrettyName() {
    return prettyName;
  }

  public String getName() {
    return name;
  }

  public String getPluralName() {
    return pluralName;
  }

  public ProgramType getProgramType() {
    return programType;
  }

  public ProgramType getParentType() {
    return parentType;
  }

  public String getPluralPrettyName() {
    return pluralPrettyName;
  }

  public boolean canScale() {
    return capabilities.contains(Capability.SCALE);
  }

  public boolean hasHistory() {
    return capabilities.contains(Capability.HISTORY);
  }

  public boolean hasLogs() {
    return capabilities.contains(Capability.LOGS);
  }

  public boolean hasLiveInfo() {
    return capabilities.contains(Capability.LIVE_INFO);
  }

  public boolean hasStatus() {
    return capabilities.contains(Capability.STATUS);
  }

  public boolean canStartStop() {
    return capabilities.contains(Capability.START_STOP);
  }

  public static ElementType fromProgramType(ProgramType programType) {
    for (ElementType elementType : ElementType.values()) {
      if (elementType.getProgramType() == programType) {
        return elementType;
      }
    }
    throw new IllegalArgumentException("Invalid ElementType from ProgramType " + programType);
  }

  private enum Capability {
    SCALE, HISTORY, LOGS, LIVE_INFO, STATUS, START_STOP
  }
}
