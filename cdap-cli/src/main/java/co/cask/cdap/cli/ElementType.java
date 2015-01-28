/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.cli;

import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Represents types of programs and their elements.
 */
public enum ElementType {

  ADAPTER("Adapter", "Adapters", "adapter", "adapters",
          null, null, ArgumentName.ADAPTER, CommandCategory.ADAPTERS),

  INSTANCE("Instance", "Instance", "instance", "instance",
           null, null, ArgumentName.INSTANCE, CommandCategory.PREFERENCES, Capability.PREFERENCES),

  NAMESPACE("Namespace", "Namespaces", "namespace", "namespaces",
            null, null, ArgumentName.NAMESPACE_ID, CommandCategory.NAMESPACES, Capability.PREFERENCES),

  APP("application", "applications", "app", "apps",
      null, null, ArgumentName.APP, CommandCategory.APPS, Capability.LIST, Capability.PREFERENCES),

  DATASET("Dataset", "Datasets", "dataset", "datasets",
          null, null, ArgumentName.DATASET, CommandCategory.DATASETS, Capability.LIST),

  DATASET_MODULE("Dataset module", "Dataset modules", "dataset module", "dataset modules",
                 null, null, ArgumentName.DATASET_MODULE, CommandCategory.DATASETS, Capability.LIST),

  DATASET_TYPE("Dataset type", "Dataset types", "dataset type", "dataset types",
               null, null, ArgumentName.DATASET_TYPE, CommandCategory.DATASETS, Capability.LIST),

  QUERY("Dataset query", "Dataset queries", "dataset query", "dataset queries",
        null, null, ArgumentName.QUERY, CommandCategory.EXPLORE),

  STREAM("Stream", "Streams", "stream", "streams",
         null, null, ArgumentName.STREAM, CommandCategory.STREAMS, Capability.LIST),

  PROGRAM("program", "programs", "program", "programs",
          null, null, ArgumentName.PROGRAM, CommandCategory.PROGRAMS),

  FLOW("Flow", "Flows", "flow", "flows",
       ProgramType.FLOW, null,
       ArgumentName.FLOW, CommandCategory.FLOWS,
       Capability.RUNS, Capability.LOGS, Capability.LIVE_INFO, Capability.STATUS, Capability.START_STOP,
       Capability.LIST, Capability.RUNTIME_ARGS, Capability.PREFERENCES),

  WORKFLOW("Workflow", "Workflows", "workflow", "workflows",
           ProgramType.WORKFLOW, null,
           ArgumentName.WORKFLOW, CommandCategory.WORKFLOWS,
           Capability.RUNS, Capability.STATUS, Capability.START_STOP,
           Capability.LIST, Capability.RUNTIME_ARGS, Capability.PREFERENCES),

  FLOWLET("Flowlet", "Flowlets", "flowlet", "flowlets",
          null, ProgramType.FLOW,
          ArgumentName.FLOWLET, CommandCategory.FLOWS,
          Capability.SCALE),

  PROCEDURE("Procedure", "Procedures", "procedure", "procedures",
            ProgramType.PROCEDURE, null,
            ArgumentName.PROCEDURE, CommandCategory.PROCEDURES,
            Capability.RUNS, Capability.SCALE, Capability.LOGS, Capability.LIVE_INFO, Capability.STATUS,
            Capability.START_STOP, Capability.LIST, Capability.RUNTIME_ARGS, Capability.PREFERENCES),

  SERVICE("Service", "Services", "service", "services",
          ProgramType.SERVICE, null,
          ArgumentName.SERVICE, CommandCategory.SERVICES,
          Capability.START_STOP, Capability.STATUS, Capability.LIST, Capability.RUNTIME_ARGS, Capability.PREFERENCES),

  RUNNABLE("Runnable", "Runnables", "runnable", "runnables",
           null, ProgramType.SERVICE,
           ArgumentName.RUNNABLE, CommandCategory.SERVICES,
           Capability.SCALE, Capability.RUNS, Capability.LOGS),

  MAPREDUCE("MapReduce Program", "MapReduce Programs", "mapreduce program", "mapreduce programs",
            ProgramType.MAPREDUCE, null,
            ArgumentName.MAPREDUCE, CommandCategory.MAPREDUCE,
            Capability.LOGS, Capability.RUNS, Capability.STATUS, Capability.START_STOP, Capability.LIST,
            Capability.RUNTIME_ARGS, Capability.PREFERENCES),

  SPARK("Spark Program", "Spark Programs", "spark program", "spark programs",
            ProgramType.SPARK, null,
            ArgumentName.SPARK, CommandCategory.SPARK,
            Capability.LOGS, Capability.RUNS, Capability.STATUS, Capability.START_STOP, Capability.LIST,
            Capability.RUNTIME_ARGS, Capability.PREFERENCES);

  private final String pluralName;
  private final String pluralPrettyName;
  private final String name;
  private final ProgramType programType;
  private final ProgramType parentType;
  private final Set<Capability> capabilities;
  private final String prettyName;
  private final ArgumentName argumentName;
  private final CommandCategory commandCategory;

  ElementType(String prettyName, String pluralPrettyName, String name, String pluralName,
              ProgramType programType, ProgramType parentType, ArgumentName argumentName,
              CommandCategory commandCategory, Capability... capabilities) {
    this.prettyName = prettyName;
    this.pluralPrettyName = pluralPrettyName;
    this.name = name;
    this.pluralName = pluralName;
    this.programType = programType;
    this.parentType = parentType;
    this.argumentName = argumentName;
    this.commandCategory = commandCategory;
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

  public ArgumentName getArgumentName() {
    return argumentName;
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

  public boolean hasRuns() {
    return capabilities.contains(Capability.RUNS);
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

  public boolean isListable() {
    return capabilities.contains(Capability.LIST);
  }

  public boolean hasRuntimeArgs() {
    return capabilities.contains(Capability.RUNTIME_ARGS);
  }

  public boolean hasPreferences() {
    return capabilities.contains(Capability.PREFERENCES);
  }

  public CommandCategory getCommandCategory() {
    return commandCategory;
  }

  private enum Capability {
    SCALE, RUNS, LOGS, LIVE_INFO, STATUS, START_STOP, LIST, RUNTIME_ARGS, PREFERENCES
  }
}
