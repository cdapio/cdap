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

/**
 * Represents a record of a program run information to be included in a dashboard detail view.
 */
public class DashboardProgramRunRecord {
  private final String namespace;
  private final ArtifactMetaInfo artifact;
  private final String application;
  private final String program;
  private final long duration;
  private final String user;
  private final String startMethod;
  private final ProgramRunStatus status;

  public DashboardProgramRunRecord(String namespace, ArtifactMetaInfo artifact, String application, String program,
                                   long duration, String user, String startMethod, ProgramRunStatus status) {
    this.namespace = namespace;
    this.artifact = artifact;
    this.application = application;
    this.program = program;
    this.duration = duration;
    this.user = user;
    this.startMethod = startMethod;
    this.status = status;
  }

  public String getNamespace() {
    return namespace;
  }

  public ArtifactMetaInfo getArtifact() {
    return artifact;
  }

  public String getApplication() {
    return application;
  }

  public String getProgram() {
    return program;
  }

  public long getDuration() {
    return duration;
  }

  public String getUser() {
    return user;
  }

  public String getStartMethod() {
    return startMethod;
  }

  public ProgramRunStatus getStatus() {
    return status;
  }
}
