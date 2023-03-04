/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.spi.events;

import javax.annotation.Nullable;

/**
 * {@link Event} implementation for Program Status
 */
public class ProgramStatusEvent implements Event<ProgramStatusEventDetails> {

  private final long publishTime;
  private final String version;
  private final String instanceName;
  private final String projectName;
  private final ProgramStatusEventDetails programStatusEventDetails;

  public ProgramStatusEvent(long publishTime, String version, String instanceName,
      @Nullable String projectName,
      ProgramStatusEventDetails programStatusEventDetails) {
    this.publishTime = publishTime;
    this.version = version;
    this.instanceName = instanceName;
    this.projectName = projectName;
    this.programStatusEventDetails = programStatusEventDetails;
  }

  @Override
  public EventType getType() {
    return EventType.PROGRAM_STATUS;
  }

  @Override
  public long getPublishTime() {
    return publishTime;
  }

  @Override
  public String getVersion() {
    return version;
  }

  @Override
  public String getInstanceName() {
    return instanceName;
  }

  @Override
  public String getProjectName() {
    return projectName;
  }

  @Override
  public ProgramStatusEventDetails getEventDetails() {
    return programStatusEventDetails;
  }

  @Override
  public String toString() {
    return "ProgramStatusEvent{"
        + "publishTime=" + publishTime
        + ", version='" + version + '\''
        + ", instanceName='" + instanceName + '\''
        + ", projectName='" + projectName + '\''
        + ", programStatusEventDetails=" + programStatusEventDetails.toString()
        + '}';
  }
}
