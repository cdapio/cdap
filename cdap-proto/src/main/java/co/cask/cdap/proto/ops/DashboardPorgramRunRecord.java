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
public class DashboardPorgramRunRecord {
  private final String namespace;
  private final String programName;
  private final String type;
  private final long durationTs;
  private final String user;
  private final String startMethod;
  private final ProgramRunStatus status;

  public DashboardPorgramRunRecord(String namespace, String programName, String type,
                                   long durationTs, String user, String startMethod, ProgramRunStatus status) {
    this.namespace = namespace;
    this.programName = programName;
    this.type = type;
    this.durationTs = durationTs;
    this.user = user;
    this.startMethod = startMethod;
    this.status = status;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getProgramName() {
    return programName;
  }

  public String getType() {
    return type;
  }

  public long getDurationTs() {
    return durationTs;
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
