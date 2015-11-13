/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.proto;

import com.google.common.base.Joiner;

import java.util.Objects;

/**
 * Array components of the batch status request to POST /namespaces/{namespace}/status.
 */
public class BatchProgram {
  protected final String appId;
  protected final ProgramType programType;
  protected final String programId;

  public BatchProgram(String appId, ProgramType programType, String programId) {
    this.appId = appId;
    this.programType = programType;
    this.programId = programId;
  }

  public static BatchProgram from(ProgramRecord programRecord) {
    return new BatchProgram(programRecord.getApp(), programRecord.getType(), programRecord.getName());
  }

  public String getAppId() {
    return appId;
  }

  public ProgramType getProgramType() {
    return programType;
  }

  public String getProgramId() {
    return programId;
  }

  /**
   * Since this is often created through gson deserialization, check that all the fields are present and valid.
   *
   * @throws IllegalArgumentException if there is a field that is null or empty or invalid.
   */
  public void validate() throws IllegalArgumentException {
    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("'appId' must be specified.");
    }
    if (programType == null) {
      throw new IllegalArgumentException("'programType' must be one of " + Joiner.on(",").join(ProgramType.values()));
    }
    if (programId == null || programId.isEmpty()) {
      throw new IllegalArgumentException("'programId' must be specified.");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BatchProgram that = (BatchProgram) o;

    return Objects.equals(appId, that.appId) &&
      Objects.equals(programType, that.programType) &&
      Objects.equals(programId, that.programId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(appId, programType, programId);
  }
}
