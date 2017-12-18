/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.datastreams;

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.spec.PipelineSpec;
import co.cask.cdap.etl.spec.StageSpec;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Spec for data streams pipelines.
 */
public class DataStreamsPipelineSpec extends PipelineSpec {
  private final long batchIntervalMillis;
  private final String extraJavaOpts;
  private final boolean stopGracefully;
  private final boolean checkpointsDisabled;
  private final boolean isUnitTest;
  private final String checkpointDirectory;

  private DataStreamsPipelineSpec(Set<StageSpec> stages, Set<Connection> connections,
                                  Resources resources, Resources driverResources, Resources clientResources,
                                  boolean stageLoggingEnabled, boolean processTimingEnabled, long batchIntervalMillis,
                                  String extraJavaOpts, int numOfRecordsPreview,
                                  boolean stopGracefully, Map<String, String> properties,
                                  boolean checkpointsDisabled, boolean isUnitTest, String checkpointDirectory) {
    super(stages, connections, resources, driverResources, clientResources, stageLoggingEnabled, processTimingEnabled,
          numOfRecordsPreview, properties);
    this.batchIntervalMillis = batchIntervalMillis;
    this.extraJavaOpts = extraJavaOpts;
    this.stopGracefully = stopGracefully;
    this.checkpointsDisabled = checkpointsDisabled;
    this.isUnitTest = isUnitTest;
    this.checkpointDirectory = checkpointDirectory;
  }

  public long getBatchIntervalMillis() {
    return batchIntervalMillis;
  }

  public String getExtraJavaOpts() {
    return extraJavaOpts;
  }

  public boolean isStopGracefully() {
    return stopGracefully;
  }

  public boolean isCheckpointsDisabled() {
    return checkpointsDisabled;
  }

  public boolean isUnitTest() {
    return isUnitTest;
  }

  public String getCheckpointDirectory() {
    return checkpointDirectory;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    DataStreamsPipelineSpec that = (DataStreamsPipelineSpec) o;

    return batchIntervalMillis == that.batchIntervalMillis &&
      Objects.equals(extraJavaOpts, that.extraJavaOpts) &&
      stopGracefully == that.stopGracefully &&
      checkpointsDisabled == that.checkpointsDisabled &&
      isUnitTest == that.isUnitTest &&
      Objects.equals(checkpointDirectory, that.checkpointDirectory);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), batchIntervalMillis, extraJavaOpts,
                        stopGracefully, checkpointsDisabled, isUnitTest, checkpointDirectory);
  }

  @Override
  public String toString() {
    return "DataStreamsPipelineSpec{" +
      "batchIntervalMillis=" + batchIntervalMillis +
      ", extraJavaOpts='" + extraJavaOpts + '\'' +
      ", stopGracefully=" + stopGracefully +
      ", checkpointsDisabled=" + checkpointsDisabled +
      ", isUnitTest=" + isUnitTest +
      ", checkpointDirectory='" + checkpointDirectory + '\'' +
      "} " + super.toString();
  }

  public static Builder builder(long batchIntervalMillis) {
    return new Builder(batchIntervalMillis);
  }

  /**
   * Builder for creating a BatchPipelineSpec.
   */
  public static class Builder extends PipelineSpec.Builder<Builder> {
    private final long batchIntervalMillis;
    private String extraJavaOpts;
    private boolean stopGracefully;
    private boolean checkpointsDisabled;
    private boolean isUnitTest;
    private String checkpointDirectory;

    public Builder(long batchIntervalMillis) {
      this.batchIntervalMillis = batchIntervalMillis;
      this.stopGracefully = false;
      this.checkpointsDisabled = false;
      this.isUnitTest = false;
      this.checkpointDirectory = UUID.randomUUID().toString();
    }

    public Builder setExtraJavaOpts(String extraJavaOpts) {
      this.extraJavaOpts = extraJavaOpts;
      return this;
    }

    public Builder setStopGracefully(boolean stopGracefully) {
      this.stopGracefully = stopGracefully;
      return this;
    }

    public Builder setCheckpointsDisabled(boolean checkpointsDisabled) {
      this.checkpointsDisabled = checkpointsDisabled;
      return this;
    }

    public Builder setCheckpointDirectory(String checkpointDirectory) {
      this.checkpointDirectory = checkpointDirectory;
      return this;
    }

    public Builder setIsUnitTest(boolean isUnitTest) {
      this.isUnitTest = isUnitTest;
      return this;
    }

    @Override
    public DataStreamsPipelineSpec build() {
      return new DataStreamsPipelineSpec(stages, connections, resources, driverResources, clientResources,
                                         stageLoggingEnabled, processTimingEnabled, batchIntervalMillis, extraJavaOpts,
                                         numOfRecordsPreview, stopGracefully, properties,
                                         checkpointsDisabled, isUnitTest, checkpointDirectory);
    }
  }
}
