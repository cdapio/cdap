/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.proto.ops;

import co.cask.cdap.proto.ProgramRunStatus;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents a record of a program run information to be included in a report.
 */
public class ReportProgramRunRecord {
  @Nullable
  private final String namespace;
  private final String programName;
  private final String type;
  @Nullable
  private final ProgramRunStatus status;
  @Nullable
  private final long startTs;
  @Nullable
  private final long endTs;
  @Nullable
  private final long durationTs;
  @Nullable
  private final String user;
  @Nullable
  private final String startMethod;
  @Nullable
  private final Map<String, String> runtimeArgs;
  @Nullable
  private final int minMemory;
  @Nullable
  private final int maxMemory;
  @Nullable
  private final int averageMemory;
  @Nullable
  private final int minNumCores;
  @Nullable
  private final int maxNumCores;
  @Nullable
  private final int averageNumCores;
  @Nullable
  private final int minNumContainers;
  @Nullable
  private final int maxNumContainers;
  @Nullable
  private final int averageNumContainers;
  @Nullable
  private final int numLogWarnings;
  @Nullable
  private final int numLogErrors;
  @Nullable
  private final int numRecordsOut;

  public ReportProgramRunRecord(String namespace, String programName, String type, ProgramRunStatus status,
                                long startTs, long endTs, long durationTs, String user, String startMethod,
                                Map<String, String> runtimeArgs, int minMemory, int maxMemory, int averageMemory,
                                int minNumCores, int maxNumCores, int averageNumCores,
                                int minNumContainers, int maxNumContainers, int averageNumContainers,
                                int numLogWarnings, int numLogErrors, int numRecordsOut) {
    this.namespace = namespace;
    this.programName = programName;
    this.type = type;
    this.status = status;
    this.startTs = startTs;
    this.endTs = endTs;
    this.durationTs = durationTs;
    this.user = user;
    this.startMethod = startMethod;
    this.runtimeArgs = runtimeArgs;
    this.minMemory = minMemory;
    this.maxMemory = maxMemory;
    this.averageMemory = averageMemory;
    this.minNumCores = minNumCores;
    this.maxNumCores = maxNumCores;
    this.averageNumCores = averageNumCores;
    this.minNumContainers = minNumContainers;
    this.maxNumContainers = maxNumContainers;
    this.averageNumContainers = averageNumContainers;
    this.numLogWarnings = numLogWarnings;
    this.numLogErrors = numLogErrors;
    this.numRecordsOut = numRecordsOut;
  }
}