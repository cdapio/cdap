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
  private final String status;
  @Nullable
  private final Long start;
  @Nullable
  private final Long running;
  @Nullable
  private final Long end;
  @Nullable
  private final Long duration;
  @Nullable
  private final String user;
  @Nullable
  private final String startMethod;
  @Nullable
  private final Map<String, String> runtimeArgs;
  @Nullable
  private final Integer numLogWarnings;
  @Nullable
  private final Integer numLogErrors;
  @Nullable
  private final Integer numRecordsOut;

  public ProgramRunReportRecord(String namespace, ArtifactMetaInfo artifact, ApplicationMetaInfo application,
                                String programName, String status,
                                long start, long running, long end, long duration, String user, String startMethod,
                                Map<String, String> runtimeArgs,
                                int numLogWarnings, int numLogErrors, int numRecordsOut) {
    this.namespace = namespace;
    this.artifact = artifact;
    this.application = application;
    this.programName = programName;
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
  public String getStatus() {
    return status;
  }

  @Nullable
  public Long getStart() {
    return start;
  }

  @Nullable
  public Long getRunning() {
    return running;
  }

  @Nullable
  public Long getEnd() {
    return end;
  }

  @Nullable
  public Long getDuration() {
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
  public Integer getNumLogWarnings() {
    return numLogWarnings;
  }

  @Nullable
  public Integer getNumLogErrors() {
    return numLogErrors;
  }

  @Nullable
  public Integer getNumRecordsOut() {
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
