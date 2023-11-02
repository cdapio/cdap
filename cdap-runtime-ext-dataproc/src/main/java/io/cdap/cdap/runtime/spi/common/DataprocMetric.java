/*
 *  Copyright © 2023 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.cdap.runtime.spi.common;

import io.cdap.cdap.runtime.spi.runtimejob.LaunchMode;
import javax.annotation.Nullable;

/**
 * Dataproc related metric.
 */
public class DataprocMetric {
  private final String region;
  private final String metricName;
  @Nullable
  private final Exception exception;
  @Nullable
  private final LaunchMode launchMode;

  private DataprocMetric(String metricName, String region, @Nullable Exception exception,
      @Nullable LaunchMode launchMode) {
    this.metricName = metricName;
    this.region = region;
    this.exception = exception;
    this.launchMode = launchMode;
  }

  public String getMetricName() {
    return metricName;
  }

  public String getRegion() {
    return region;
  }

  @Nullable
  public Exception getException() {
    return exception;
  }

  @Nullable
  public LaunchMode getLaunchMode() {
    return launchMode;
  }

  /**
   * Returns a builder to create a DataprocMetric.
   *
   * @param metricName metric name
   * @return Builder to create a DataprocMetric
   */
  public static Builder builder(String metricName) {
    return new Builder(metricName);
  }

  /**
   * Builder for a DataprocMetric.
   */
  public static class Builder {
    private final String metricName;
    private String region;
    @Nullable
    private Exception exception;
    @Nullable
    private LaunchMode launchMode;

    private Builder(String metricName) {
      this.metricName = metricName;
    }

    public Builder setRegion(String region) {
      this.region = region;
      return this;
    }

    public Builder setException(@Nullable Exception e) {
      this.exception = e;
      return this;
    }

    public Builder setLaunchMode(@Nullable LaunchMode launchMode) {
      this.launchMode = launchMode;
      return this;
    }

    /**
     * Returns a DataprocMetric.
     *
     * @return DataprocMetric.
     */
    public DataprocMetric build() {
      if (region == null) {
        // region should always be set unless there is a bug in the code
        throw new IllegalStateException("Dataproc metric is missing the region");
      }
      return new DataprocMetric(metricName, region, exception, launchMode);
    }
  }
}
