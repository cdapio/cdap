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

package co.cask.cdap.examples.report;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 *
 */
public class ReportRecordBuilder implements Serializable {
  private String program;
  private String run;
  private List<StatusTime> statusTimes;
  @Nullable
  private ProgramStartingInfo programStartingInfo;

  public ReportRecordBuilder() {
    this.statusTimes = new ArrayList<>();
  }

  public void setProgramRunStatus(String program, String run, String status, long time, @Nullable String user) {
    this.program = program;
    this.run = run;
    StatusTime statusTime = new StatusTime();
    statusTime.setStatus(status);
    statusTime.setTime(time);
    this.statusTimes.add(statusTime);
    if (user != null) {
      this.programStartingInfo = new ProgramStartingInfo();
      this.programStartingInfo.setUser(user);
    }
  }

  public ReportRecordBuilder merge(ReportRecordBuilder other) {
    this.statusTimes.addAll(other.statusTimes);
    this.programStartingInfo = this.programStartingInfo != null ?
      this.programStartingInfo : other.programStartingInfo;
    return this;
  }

  public String getProgram() {
    return program;
  }

  public void setProgram(String program) {
    this.program = program;
  }

  public String getRun() {
    return run;
  }

  public void setRun(String run) {
    this.run = run;
  }

  public List<StatusTime> getStatusTimes() {
    return statusTimes;
  }

  public void setStatusTimes(List<StatusTime> statusTimes) {
    this.statusTimes = statusTimes;
  }

  @Nullable
  public ProgramStartingInfo getProgramStartingInfo() {
    return programStartingInfo;
  }

  public void setProgramStartingInfo(@Nullable ProgramStartingInfo programStartingInfo) {
    this.programStartingInfo = programStartingInfo;
  }
}
