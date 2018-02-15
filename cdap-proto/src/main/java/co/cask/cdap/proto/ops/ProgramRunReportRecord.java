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
public class ProgramRunReportRecord {
  @Nullable
  private final String namespace;
  private final ArtifactMetaInfo artifact;
  private final ApplicationMetaInfo application;
  private final String programName;
  @Nullable
  private final ProgramRunStatus status;
  @Nullable
  private final long start;
  @Nullable
  private final long end;
  @Nullable
  private final long duration;
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
  private final double averageMemory;
  @Nullable
  private final int minNumCores;
  @Nullable
  private final int maxNumCores;
  @Nullable
  private final double averageNumCores;
  @Nullable
  private final int minNumContainers;
  @Nullable
  private final int maxNumContainers;
  @Nullable
  private final double averageNumContainers;
  @Nullable
  private final int numLogWarnings;
  @Nullable
  private final int numLogErrors;
  @Nullable
  private final int numRecordsOut;

  public ProgramRunReportRecord(String namespace, ArtifactMetaInfo artifact, ApplicationMetaInfo application,
                                String programName, ProgramRunStatus status, long start, long end, long duration,
                                String user, String startMethod, Map<String, String> runtimeArgs,
                                int minMemory, int maxMemory, double averageMemory,
                                int minNumCores, int maxNumCores, double averageNumCores,
                                int minNumContainers, int maxNumContainers, double averageNumContainers,
                                int numLogWarnings, int numLogErrors, int numRecordsOut) {
    this.namespace = namespace;
    this.artifact = artifact;
    this.application = application;
    this.programName = programName;
    this.status = status;
    this.start = start;
    this.end = end;
    this.duration = duration;
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

  @Nullable
  public String getNamespace() {
    return namespace;
  }

  public ArtifactMetaInfo getArtifact() {
    return artifact;
  }

  public ApplicationMetaInfo getApplication() {
    return application;
  }

  public String getProgramName() {
    return programName;
  }

  @Nullable
  public ProgramRunStatus getStatus() {
    return status;
  }

  @Nullable
  public long getStart() {
    return start;
  }

  @Nullable
  public long getEnd() {
    return end;
  }

  @Nullable
  public long getDuration() {
    return duration;
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public String getStartMethod() {
    return startMethod;
  }

  @Nullable
  public Map<String, String> getRuntimeArgs() {
    return runtimeArgs;
  }

  @Nullable
  public int getMinMemory() {
    return minMemory;
  }

  @Nullable
  public int getMaxMemory() {
    return maxMemory;
  }

  @Nullable
  public double getAverageMemory() {
    return averageMemory;
  }

  @Nullable
  public int getMinNumCores() {
    return minNumCores;
  }

  @Nullable
  public int getMaxNumCores() {
    return maxNumCores;
  }

  @Nullable
  public double getAverageNumCores() {
    return averageNumCores;
  }

  @Nullable
  public int getMinNumContainers() {
    return minNumContainers;
  }

  @Nullable
  public int getMaxNumContainers() {
    return maxNumContainers;
  }

  @Nullable
  public double getAverageNumContainers() {
    return averageNumContainers;
  }

  @Nullable
  public int getNumLogWarnings() {
    return numLogWarnings;
  }

  @Nullable
  public int getNumLogErrors() {
    return numLogErrors;
  }

  @Nullable
  public int getNumRecordsOut() {
    return numRecordsOut;
  }

  /**
   * Represents the application meta information of a program run.
   */
  public static class ApplicationMetaInfo {
    private final String name;
    private final String version;

    public ApplicationMetaInfo(String name, String version) {
      this.name = name;
      this.version = version;
    }

    public String getName() {
      return name;
    }

    public String getVersion() {
      return version;
    }
  }
}
