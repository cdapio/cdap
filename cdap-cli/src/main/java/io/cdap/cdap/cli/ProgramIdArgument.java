/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.cli;

import com.google.common.base.MoreObjects;

/**
 * Represents a program information, such as the app id and program id.
 */
public class ProgramIdArgument {

  private String appId;
  private String programId;

  public ProgramIdArgument(String appId, String programId) {
    this.appId = appId;
    this.programId = programId;
  }

  public String getAppId() {
    return appId;
  }

  public String getProgramId() {
    return programId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ProgramIdArgument)) {
      return false;
    }

    ProgramIdArgument other = (ProgramIdArgument) o;
    return java.util.Objects.equals(appId, other.getAppId())
        && java.util.Objects.equals(programId, other.getProgramId());
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(appId, programId);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("appId", appId)
        .add("programId", programId)
        .toString();
  }
}
