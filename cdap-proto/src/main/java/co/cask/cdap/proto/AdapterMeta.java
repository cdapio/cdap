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

package co.cask.cdap.proto;

import co.cask.cdap.api.schedule.Schedule;
import com.google.common.base.Objects;

/**
 * Represents metadata for Adapters
 */
public final class AdapterMeta {
  //TODO: make interface (For generic adapter types)
  private final String id;
  private final String streamName;
  private final String datasetName;
  private final String frequency;
  // TODO: non-hardcode the following 3
  private static final String appId = "TODO: replace this";
  private static final String programId = "TODO: replace this";
  private static final ProgramType programType = ProgramType.WORKFLOW;

  public AdapterMeta(String id, String streamName, String datasetName, String frequency) {
    this.id = id;
    this.streamName = streamName;
    this.datasetName = datasetName;
    this.frequency = frequency;
  }

  public String getId() {
    return id;
  }

  public String getStreamName() {
    return streamName;
  }

  public String getDatasetName() {
    return datasetName;
  }

  public String getFrequency() {
    throw new UnsupportedOperationException("Frequency is currently not supported");
  }

  public Schedule getSchedule() {
    throw new UnsupportedOperationException("schedule is currently not supported");
  }

  public String getAppId() {
    return appId;
  }

  public String getProgramId() {
    return programId;
  }

  public ProgramType getProgramType() {
    return programType;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("id", id)
      .add("streamName", streamName)
      .add("datasetName", datasetName)
      .add("frequency", frequency)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AdapterMeta that = (AdapterMeta) o;

    return Objects.equal(this.id, that.id) &&
      Objects.equal(this.streamName, that.streamName) &&
      Objects.equal(this.datasetName, that.datasetName) &&
      Objects.equal(this.frequency, that.frequency);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, streamName, datasetName, frequency);
  }
}
