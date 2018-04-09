/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.registry;

import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;

import javax.annotation.Nullable;

/**
 * Class for carrying dataset usage information.
 */
public final class DatasetUsage {

  private final ProgramId programId;
  @Nullable
  private final DatasetId datasetId;
  @Nullable
  private final StreamId streamId;

  public DatasetUsage(ProgramId programId, @Nullable DatasetId datasetId, @Nullable StreamId streamId) {
    this.programId = programId;
    this.datasetId = datasetId;
    this.streamId = streamId;
  }

  /**
   * Returns the program who uses the dataset/stream.
   */
  public ProgramId getProgramId() {
    return programId;
  }

  /**
   * Returns the {@link DatasetId} being used, or {@code null} if the usage is not about dataset.
   */
  @Nullable
  public DatasetId getDatasetId() {
    return datasetId;
  }

  /**
   * Returns the {@link StreamId} being used, or {@code null} if the usage is not about stream.
   */
  @Nullable
  public StreamId getStreamId() {
    return streamId;
  }

  @Override
  public String toString() {
    return "DatasetUsage{" +
      "programId=" + programId +
      ", datasetId=" + datasetId +
      ", streamId=" + streamId +
      '}';
  }
}
