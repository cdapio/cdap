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

import co.cask.cdap.cli.english.Noun;
import co.cask.cdap.cli.english.Word;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Represents types of programs and their elements.
 */
public enum ElementType {

  ADAPTER(new Noun("adapter"), new Noun("Adapter"), null, null,
          ArgumentName.ADAPTER),

  INSTANCE(new Noun("instance"), new Noun("Instance"), null, null,
           ArgumentName.INSTANCE, Capability.PREFERENCES),

  NAMESPACE(new Noun("namespace"), new Noun("Namespace"), null, null,
            ArgumentName.NAMESPACE_NAME, Capability.PREFERENCES),

  APP(new Noun("app"), new Noun("Application"), null, null,
      ArgumentName.APP, Capability.LIST, Capability.PREFERENCES),

  APP_TEMPLATE(new Noun("app-template"), new Noun("Application Template"), null, null,
      ArgumentName.APP_TEMPLATE, Capability.LIST),

  DATASET(new Noun("dataset"), new Noun("Dataset"), null, null,
          ArgumentName.DATASET, Capability.LIST),

  DATASET_MODULE(new Noun("dataset module"), new Noun("Dataset module"), null, null,
                 ArgumentName.DATASET_MODULE, Capability.LIST),

  DATASET_TYPE(new Noun("dataset type"), new Noun("Dataset type"), null, null,
               ArgumentName.DATASET_TYPE, Capability.LIST),

  QUERY(new Noun("query"), new Noun("Query"), null, null, ArgumentName.QUERY),

  STREAM(new Noun("stream"), new Noun("Stream"), null, null, ArgumentName.STREAM, Capability.LIST),

  PROGRAM(new Noun("program"), new Noun("Program"), null, null, ArgumentName.PROGRAM),

  FLOW(new Noun("flow"), new Noun("Flow"), ProgramType.FLOW, null, ArgumentName.FLOW,
       Capability.RUNS, Capability.LOGS, Capability.LIVE_INFO, Capability.STATUS, Capability.START, Capability.STOP,
       Capability.LIST, Capability.RUNTIME_ARGS, Capability.PREFERENCES),

  WORKFLOW(new Noun("workflow"), new Noun("Workflow"), ProgramType.WORKFLOW, null,
           ArgumentName.WORKFLOW,
           Capability.RUNS, Capability.STATUS, Capability.START,
           Capability.LIST, Capability.RUNTIME_ARGS, Capability.PREFERENCES),

  FLOWLET(new Noun("flowlet"), new Noun("Flowlet"), null, ProgramType.FLOW,
          ArgumentName.FLOWLET,
          Capability.SCALE),

  WORKER(new Noun("worker"), new Noun("Worker"), ProgramType.WORKER, null, ArgumentName.WORKER,
         Capability.RUNS, Capability.SCALE, Capability.LOGS, Capability.LIVE_INFO, Capability.STATUS,
         Capability.START, Capability.STOP, Capability.LIST, Capability.RUNTIME_ARGS, Capability.PREFERENCES),

  SERVICE(new Noun("service"), new Noun("Service"), ProgramType.SERVICE, null,
          ArgumentName.SERVICE,
          Capability.START, Capability.STOP, Capability.STATUS, Capability.LIST,
          Capability.RUNTIME_ARGS, Capability.PREFERENCES, Capability.RUNS, Capability.SCALE),

  MAPREDUCE(new Noun("mapreduce", "mapreduce programs"), new Noun("MapReduce Program", "MapReduce Programs"),
            ProgramType.MAPREDUCE, null,
            ArgumentName.MAPREDUCE,
            Capability.LOGS, Capability.RUNS, Capability.STATUS, Capability.START, Capability.STOP, Capability.LIST,
            Capability.RUNTIME_ARGS, Capability.PREFERENCES),

  SPARK(new Noun("spark", "spark programs"), new Noun("Spark Program", "Spark Programs"),
            ProgramType.SPARK, null,
            ArgumentName.SPARK,
            Capability.LOGS, Capability.RUNS, Capability.STATUS, Capability.START, Capability.STOP, Capability.LIST,
            Capability.RUNTIME_ARGS, Capability.PREFERENCES);

  private final Noun name;
  private final ProgramType programType;
  private final ProgramType parentType;
  private final Set<Capability> capabilities;
  private final ArgumentName argumentName;
  private final Noun titleName;

  ElementType(Noun name, Noun titleName, ProgramType programType, ProgramType parentType,
              ArgumentName argumentName,
              Capability... capabilities) {
    this.name = name;
    this.titleName = titleName;
    this.programType = programType;
    this.parentType = parentType;
    this.argumentName = argumentName;
    this.capabilities = Sets.newHashSet(capabilities);
  }

  public boolean isTopLevel() {
    return parentType == null;
  }

  public Word getName() {
    return name.getName();
  }

  public Word getNamePlural() {
    return name.getNamePlural();
  }

  public Word getTitleName() {
    return titleName.getName();
  }

  public Word getTitleNamePlural() {
    return titleName.getNamePlural();
  }

  public ArgumentName getArgumentName() {
    return argumentName;
  }

  public ProgramType getProgramType() {
    return programType;
  }

  public ProgramType getParentType() {
    return parentType;
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

  public boolean canStart() {
    return capabilities.contains(Capability.START);
  }

  public boolean canStop() {
    return capabilities.contains(Capability.STOP);
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

  private enum Capability {
    SCALE, RUNS, LOGS, LIVE_INFO, STATUS, START, STOP, LIST, RUNTIME_ARGS, PREFERENCES
  }
}
