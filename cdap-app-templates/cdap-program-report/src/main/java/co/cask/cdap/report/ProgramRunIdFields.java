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

package co.cask.cdap.report;

import co.cask.cdap.api.artifact.ArtifactId;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * run id fields
 */
public class ProgramRunIdFields {
  private final String application;
  private final String version;
  private final String type;
  private final String program;
  private final String run;
  private final String namespace;

  private Long timestamp;
  private String messageId;
  private String programStatus;

  @Nullable
  private ProgramStartInfo startInfo;

  ProgramRunIdFields(String application, String version, String type, String program, String run,
                     String namespace) {
    this.application = application;
    this.version = version;
    this.type = type;
    this.program = program;
    this.run = run;
    this.namespace = namespace;
  }

  public void setTime(long timestamp) {
    this.timestamp = timestamp;
  }

  public void setStatus(String status) {
    this.programStatus = status;
  }

  public void setMessageId(String messageId) {
    this.messageId = messageId;
  }

  public void setStartInfo(ProgramStartInfo startInfo) {
    this.startInfo = startInfo;
  }

  public String getApplication() {
    return application;
  }

  public String getVersion() {
    return version;
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

  public String getNamespace() {
    return namespace;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public String getMessageId() {
    return messageId;
  }

  public String getProgramStatus() {
    return programStatus;
  }

  /**
   * this is not null only when the program status is "STARTING"
   * @return
   */
  @Nullable
  public ProgramStartInfo getProgramSartInfo() {
    return startInfo;
  }

}
