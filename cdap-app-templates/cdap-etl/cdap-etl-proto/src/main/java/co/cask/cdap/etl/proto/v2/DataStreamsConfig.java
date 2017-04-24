/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.proto.v2;

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.proto.Connection;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Data Streams Configuration.
 */
public final class DataStreamsConfig extends ETLConfig {
  private final String batchInterval;
  private final String extraJavaOpts;
  private final Boolean disableCheckpoints;
  private final String checkpointDir;
  private final Boolean stopGracefully;
  // See comments in DataStreamsSparkLauncher for explanation on why we need this.
  private final boolean isUnitTest;

  private DataStreamsConfig(Set<ETLStage> stages,
                            Set<Connection> connections,
                            Resources resources,
                            Resources driverResources,
                            Resources clientResources,
                            boolean stageLoggingEnabled,
                            boolean processTimingEnabled,
                            String batchInterval,
                            boolean isUnitTest,
                            boolean disableCheckpoints,
                            @Nullable String checkpointDir,
                            int numOfRecordsPreview,
                            boolean stopGracefully,
                            Map<String, String> properties) {
    super(stages, connections, resources, driverResources, clientResources, stageLoggingEnabled, processTimingEnabled,
          numOfRecordsPreview, properties);
    this.batchInterval = batchInterval;
    this.isUnitTest = isUnitTest;
    this.extraJavaOpts = "";
    this.disableCheckpoints = disableCheckpoints;
    this.checkpointDir = checkpointDir;
    this.stopGracefully = stopGracefully;
  }

  public String getBatchInterval() {
    return batchInterval;
  }

  public boolean isUnitTest() {
    return isUnitTest;
  }

  public boolean checkpointsDisabled() {
    return disableCheckpoints == null ? false : disableCheckpoints;
  }

  public String getExtraJavaOpts() {
    return extraJavaOpts == null || extraJavaOpts.isEmpty() ? "-XX:MaxPermSize=256m" : extraJavaOpts;
  }

  public Boolean getStopGracefully() {
    return stopGracefully == null ? true : stopGracefully;
  }

  @Nullable
  public String getCheckpointDir() {
    return checkpointDir;
  }

  @Override
  public String toString() {
    return "DataStreamsConfig{" +
      "batchInterval='" + batchInterval + '\'' +
      ", extraJavaOpts='" + extraJavaOpts + '\'' +
      ", disableCheckpoints=" + disableCheckpoints +
      ", checkpointDir='" + checkpointDir + '\'' +
      ", stopGracefully=" + stopGracefully +
      ", isUnitTest=" + isUnitTest +
      "} " + super.toString();
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

    DataStreamsConfig that = (DataStreamsConfig) o;

    return Objects.equals(batchInterval, that.batchInterval) &&
      Objects.equals(extraJavaOpts, that.extraJavaOpts) &&
      Objects.equals(disableCheckpoints, that.disableCheckpoints) &&
      Objects.equals(checkpointDir, that.checkpointDir) &&
      Objects.equals(stopGracefully, that.stopGracefully);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), batchInterval, extraJavaOpts, disableCheckpoints, checkpointDir);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder to create data stream configs.
   */
  public static class Builder extends ETLConfig.Builder<Builder> {
    private final boolean isUnitTest;
    private String batchInterval;
    private String checkpointDir;
    private boolean stopGraceFully;
    private boolean disableCheckpoints;

    public Builder() {
      this.isUnitTest = true;
      this.batchInterval = "1m";
      this.stopGraceFully = true;
      this.disableCheckpoints = false;
    }

    public Builder setBatchInterval(String batchInterval) {
      this.batchInterval = batchInterval;
      return this;
    }

    public Builder setCheckpointDir(String checkpointDir) {
      this.checkpointDir = checkpointDir;
      return this;
    }

    public Builder setStopGracefully(boolean stopGraceFully) {
      this.stopGraceFully = stopGraceFully;
      return this;
    }

    public Builder disableCheckpoints() {
      this.disableCheckpoints = true;
      return this;
    }

    public DataStreamsConfig build() {
      return new DataStreamsConfig(stages, connections, resources, driverResources, clientResources,
                                   stageLoggingEnabled, processTimingEnabled, batchInterval, isUnitTest,
                                   disableCheckpoints, checkpointDir, numOfRecordsPreview, stopGraceFully,
                                   properties);
    }
  }
}
