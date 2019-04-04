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

package io.cdap.cdap.proto.ops;

import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ProgramRunId;

import java.util.Objects;
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
  @Nullable
  private final String run;
  private final String user;
  private final String startMethod;
  private final long start;
  @Nullable
  private final Long running;
  @Nullable
  private final Long suspend;
  @Nullable
  private final Long resume;
  @Nullable
  private final Long end;
  @Nullable
  private final ProgramRunStatus status;

  public DashboardProgramRunRecord(ProgramRunId runId, RunRecord runRecord, ArtifactId artifactId,
                                   String user, String startMethod) {
    this(runId.getNamespace(), ArtifactSummary.from(artifactId),
         new ApplicationNameVersion(runId.getApplication(), runId.getVersion()),
         runId.getType().name(), runId.getProgram(), runId.getRun(), user, startMethod,
         runRecord.getStartTs(), runRecord.getRunTs(), runRecord.getSuspendTs(), runRecord.getResumeTs(),
         runRecord.getStopTs(), runRecord.getStatus());
  }

  public DashboardProgramRunRecord(String namespace, ArtifactSummary artifact, ApplicationNameVersion application,
                                   String type, String program, @Nullable String run,
                                   String user, String startMethod, long start,
                                   @Nullable Long running, @Nullable Long suspend, @Nullable Long resume,
                                   @Nullable Long end, @Nullable ProgramRunStatus status) {
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
    this.suspend = suspend;
    this.resume = resume;
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

  /**
   * @return the run id of the program run or {@code null} if the program run is scheduled to run in the future
   */
  @Nullable
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
   * @return the time in seconds when the program was last suspended, or {@code null} if the program run was not
   * suspended
   */
  @Nullable
  public Long getSuspend() {
    return suspend;
  }

  /**
   * @return the time in seconds when the program was last resumed from suspended state,
   * or {@code null} if the program run was never suspended or never resumed from suspended state.
   */
  @Nullable
  public Long getResume() {
    return resume;
  }

  /**
   * @return the time in seconds when the program run stopped, or {@code null} if the program run has not stopped yet.
   */
  @Nullable
  public Long getEnd() {
    return end;
  }

  /**
   * @return the status of the program run or {@code null} if the program run is scheduled to run in the future
   */
  @Nullable
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ApplicationNameVersion that = (ApplicationNameVersion) o;
      return Objects.equals(this.getName(), that.getName()) &&
        Objects.equals(this.getVersion(), that.getVersion());
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, version);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, artifact, application, type, program, run, user,
                        startMethod, start, running, suspend, resume, end, status);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DashboardProgramRunRecord that = (DashboardProgramRunRecord) o;
    return Objects.equals(this.getNamespace(), that.getNamespace()) &&
      Objects.equals(this.getArtifact(), that.getArtifact()) &&
      Objects.equals(this.getApplication(), that.getApplication()) &&
      Objects.equals(this.getType(), that.getType()) &&
      Objects.equals(this.getProgram(), that.getProgram()) &&
      Objects.equals(this.getRun(), that.getRun()) &&
      Objects.equals(this.getUser(), that.getUser()) &&
      Objects.equals(this.getStartMethod(), that.getStartMethod()) &&
      Objects.equals(this.getStart(), that.getStart()) &&
      Objects.equals(this.getRunning(), that.getRunning()) &&
      Objects.equals(this.getSuspend(), that.getSuspend()) &&
      Objects.equals(this.getResume(), that.getResume()) &&
      Objects.equals(this.getEnd(), that.getEnd()) &&
      Objects.equals(this.getStatus(), that.getStatus());
  }
}
