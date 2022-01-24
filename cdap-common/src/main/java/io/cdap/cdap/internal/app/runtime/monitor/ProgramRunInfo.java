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

package io.cdap.cdap.internal.app.runtime.monitor;

import io.cdap.cdap.proto.ProgramRunStatus;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Used to send information about program status from RuntimeHandler to RuntimeClient.
 */
public class ProgramRunInfo {
  private final ProgramRunStatus programRunStatus;
  @Nullable
  private final String payload;

  ProgramRunInfo(ProgramRunStatus programRunStatus, @Nullable String payload) {
    this.programRunStatus = programRunStatus;
    this.payload = payload;
  }

  public ProgramRunStatus getProgramRunStatus() {
    return programRunStatus;
  }

  @Nullable
  public String getPayload() {
    return payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProgramRunInfo that = (ProgramRunInfo) o;
    return Objects.equals(this.getProgramRunStatus(), that.getProgramRunStatus()) &&
    Objects.equals(this.getPayload(), that.getPayload());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProgramRunStatus(), getPayload());
  }

  @Override
  public String toString() {
    return "ProgramRunInfo" +
      "{programRunStatus=" + programRunStatus +
      ", payload='" + payload +
      '}';
  }
}
