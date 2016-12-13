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

import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Data Streams Configuration.
 */
public final class DataStreamsConfig extends ETLConfig {
  private final String batchInterval;
  private final Resources driverResources;
  private final String extraJavaOpts;
  private final Boolean disableCheckpoints;
  private final String checkpointDir;
  // See comments in DataStreamsSparkLauncher for explanation on why we need this.
  private final boolean isUnitTest;

  private DataStreamsConfig(Set<ETLStage> stages,
                            Set<Connection> connections,
                            Resources resources,
                            Resources driverResources,
                            boolean stageLoggingEnabled,
                            String batchInterval,
                            boolean isUnitTest,
                            boolean disableCheckpoints,
                            @Nullable String checkpointDir,
                            int numOfRecordsPreview) {
    super(stages, connections, resources, stageLoggingEnabled, numOfRecordsPreview);
    this.batchInterval = batchInterval;
    this.driverResources = driverResources;
    this.isUnitTest = isUnitTest;
    this.extraJavaOpts = "";
    this.disableCheckpoints = disableCheckpoints;
    this.checkpointDir = checkpointDir;
  }

  public Resources getDriverResources() {
    return driverResources;
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

  @Nullable
  public String getCheckpointDir() {
    return checkpointDir;
  }

  @Override
  public String toString() {
    return "DataStreamsConfig{" +
      "batchInterval='" + batchInterval + '\'' +
      ", driverResources=" + driverResources +
      ", extraJavaOpts='" + extraJavaOpts + '\'' +
      ", disableCheckpoints=" + disableCheckpoints +
      ", checkpointDir='" + checkpointDir + '\'' +
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
      Objects.equals(driverResources, that.driverResources) &&
      Objects.equals(extraJavaOpts, that.extraJavaOpts) &&
      Objects.equals(disableCheckpoints, that.disableCheckpoints) &&
      Objects.equals(checkpointDir, that.checkpointDir);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), batchInterval, driverResources,
                        extraJavaOpts, disableCheckpoints, checkpointDir);
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
    private Resources driverResources;
    private String checkpointDir;

    public Builder() {
      this.isUnitTest = true;
      this.batchInterval = "1m";
      this.driverResources = new Resources();
    }

    public Builder setBatchInterval(String batchInterval) {
      this.batchInterval = batchInterval;
      return this;
    }

    public Builder setDriverResources(Resources driverResources) {
      this.driverResources = driverResources;
      return this;
    }

    public Builder setCheckpointDir(String checkpointDir) {
      this.checkpointDir = checkpointDir;
      return this;
    }

    public DataStreamsConfig build() {
      return new DataStreamsConfig(stages, connections, resources, driverResources,
                                   stageLoggingEnabled, batchInterval, isUnitTest, false, checkpointDir,
                                   numOfRecordsPreview);
    }
  }
}
