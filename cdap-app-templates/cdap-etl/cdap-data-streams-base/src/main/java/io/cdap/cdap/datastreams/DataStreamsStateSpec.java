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

package io.cdap.cdap.datastreams;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Spec for state management for atleast once processing
 */
public class DataStreamsStateSpec {

  public enum Mode {
    NONE, SPARK_CHECKPOINTING, STATE_STORE
  }

  private final Mode mode;
  @Nullable
  private final String checkpointDir;

  public Mode getMode() {
    return mode;
  }

  @Nullable
  public String getCheckpointDir() {
    return checkpointDir;
  }

  private DataStreamsStateSpec(Mode mode, @Nullable String checkpointDir) {
    this.mode = mode;
    this.checkpointDir = checkpointDir;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DataStreamsStateSpec)) {
      return false;
    }
    DataStreamsStateSpec stateSpec = (DataStreamsStateSpec) o;
    return mode == stateSpec.mode && Objects.equals(checkpointDir, stateSpec.checkpointDir);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, checkpointDir);
  }

  @Override
  public String toString() {
    return "DataStreamsStateSpec{"
      + "mode=" + mode
      + ", checkpointDir='" + checkpointDir + '\''
      + '}';
  }

  public static Builder getBuilder(Mode mode) {
    return new Builder(mode);
  }

  public static class Builder {
    private Mode mode;
    private String checkpointDir;

    private Builder(Mode mode) {
      this.mode = mode;
    }

    public Builder setCheckPointDir(String checkpointDir) {
      this.checkpointDir = checkpointDir;
      return this;
    }

    public DataStreamsStateSpec build() {
      return new DataStreamsStateSpec(mode, checkpointDir);
    }
  }
}
