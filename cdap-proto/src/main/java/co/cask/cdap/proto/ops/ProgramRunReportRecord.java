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
  private final String namespace;
  private final ArtifactMetaInfo artifact;
  private final ApplicationMetaInfo application;
  private final String type;
  private final String program;
  @Nullable
  private final ProgramRunStatus status;
  @Nullable
  private final long start;
  @Nullable
  private final long running;
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
  private final int numLogWarnings;
  @Nullable
  private final int numLogErrors;
  @Nullable
  private final int numRecordsOut;

  public ProgramRunReportRecord(String namespace, ArtifactMetaInfo artifact, ApplicationMetaInfo application,
                                String type, String program, ProgramRunStatus status,
                                long start, long running, long end, long duration, String user, String startMethod,
                                Map<String, String> runtimeArgs,
                                int numLogWarnings, int numLogErrors, int numRecordsOut) {
    this.namespace = namespace;
    this.artifact = artifact;
    this.application = application;
    this.type = type;
    this.program = program;
    this.status = status;
    this.start = start;
    this.running = running;
    this.end = end;
    this.duration = duration;
    this.user = user;
    this.startMethod = startMethod;
    this.runtimeArgs = runtimeArgs;
    this.numLogWarnings = numLogWarnings;
    this.numLogErrors = numLogErrors;
    this.numRecordsOut = numRecordsOut;
  }

  public String getNamespace() {
    return namespace;
  }

  public ArtifactMetaInfo getArtifact() {
    return artifact;
  }

  public ApplicationMetaInfo getApplication() {
    return application;
  }

  public String getType() {
    return type;
  }

  public String getProgram() {
    return program;
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
  public long getRunning() {
    return running;
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
