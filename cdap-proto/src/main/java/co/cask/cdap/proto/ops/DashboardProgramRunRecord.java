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

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.ProgramRunId;

import javax.annotation.Nullable;

/**
 * Represents a record of a program run information to be included in a dashboard detail view.
 */
public class DashboardProgramRunRecord {
  private final String namespace;
  private final ArtifactSummary artifact;
  private final ApplicationNameVersion application;
  private final String type;
  private final String program;
  private final String run;
  private final String user;
  private final String startMethod;
  private final long start;
  @Nullable
  private final Long running;
  @Nullable
  private final Long end;
  private final ProgramRunStatus status;

  public DashboardProgramRunRecord(ProgramRunId runId, RunRecord runRecord, ArtifactId artifactId,
                                   String user, String startMethod) {
    this(runId.getNamespace(), ArtifactSummary.from(artifactId),
         new ApplicationNameVersion(runId.getApplication(), runId.getVersion()),
         runId.getType().name(), runId.getProgram(), runId.getRun(), user, startMethod,
         runRecord.getStartTs(), runRecord.getRunTs(), runRecord.getStopTs(), runRecord.getStatus());
  }

  public DashboardProgramRunRecord(String namespace, ArtifactSummary artifact, ApplicationNameVersion application,
                                   String type, String program, String run,
                                   String user, String startMethod, long start,
                                   @Nullable Long running, @Nullable Long end, ProgramRunStatus status) {
    this.namespace = namespace;
    this.artifact = artifact;
    this.application = application;
    this.type = type;
    this.program = program;
    this.run = run;
    this.user = user;
    this.startMethod = startMethod;
    this.start = start;
    this.running = running;
    this.end = end;
    this.status = status;
  }

  public String getNamespace() {
    return namespace;
  }

  public ArtifactSummary getArtifact() {
    return artifact;
  }

  public ApplicationNameVersion getApplication() {
    return application;
  }

  public String getType() {
    return type;
  }

  public String getProgram() {
    return program;
  }

  public String getRun() {
    return run;
  }

  public String getUser() {
    return user;
  }

  public String getStartMethod() {
    return startMethod;
  }

  public long getStart() {
    return start;
  }

  /**
   * @return the time in seconds when the program run started running, or {@code null} if the program run has not
   *         started running yet.
   */
  @Nullable
  public Long getRunning() {
    return running;
  }

  /**
   * @return the time in seconds when the program run stopped, or {@code null} if the program run has not stopped yet.
   */
  @Nullable
  public Long getEnd() {
    return end;
  }

  public ProgramRunStatus getStatus() {
    return status;
  }

  /**
   * Represents the name and version of an application.
   */
  public static final class ApplicationNameVersion {
    private final String name;
    private final String version;

    public ApplicationNameVersion(String name, String version) {
      this.name = name;
      this.version = version;
    }

    /**
     * @return the name of the application
     */
    public String getName() {
      return name;
    }

    /**
     * @return the version of the application
     */
    public String getVersion() {
      return version;
    }
  }
}
