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

package co.cask.cdap.datastreams;

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.spec.PipelineSpec;
import co.cask.cdap.etl.spec.StageSpec;

import java.util.Objects;
import java.util.Set;

/**
 * Spec for data streams pipelines.
 */
public class DataStreamsPipelineSpec extends PipelineSpec {
  private final long batchIntervalMillis;
  private final String extraJavaOpts;

  private DataStreamsPipelineSpec(Set<StageSpec> stages, Set<Connection> connections,
                                  Resources resources, Resources driverResources, Resources clientResources,
                                  boolean stageLoggingEnabled, long batchIntervalMillis,
                                  String extraJavaOpts, int numOfRecordsPreview) {
    super(stages, connections, resources, driverResources, clientResources, stageLoggingEnabled, numOfRecordsPreview);
    this.batchIntervalMillis = batchIntervalMillis;
    this.extraJavaOpts = extraJavaOpts;
  }

  public long getBatchIntervalMillis() {
    return batchIntervalMillis;
  }

  public String getExtraJavaOpts() {
    return extraJavaOpts;
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
      Objects.equals(extraJavaOpts, that.extraJavaOpts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), batchIntervalMillis, extraJavaOpts);
  }

  @Override
  public String toString() {
    return "DataStreamsPipelineSpec{" +
      "batchIntervalMillis=" + batchIntervalMillis +
      ", extraJavaOpts='" + extraJavaOpts + '\'' +
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

    public Builder(long batchIntervalMillis) {
      this.batchIntervalMillis = batchIntervalMillis;
    }

    public Builder setExtraJavaOpts(String extraJavaOpts) {
      this.extraJavaOpts = extraJavaOpts;
      return this;
    }

    public DataStreamsPipelineSpec build() {
      return new DataStreamsPipelineSpec(stages, connections, resources, driverResources, clientResources,
                                         stageLoggingEnabled, batchIntervalMillis, extraJavaOpts, numOfRecordsPreview);
    }
  }
}
