/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.spi.events.trigger;

import java.util.Objects;

/**
 * Represents the properties of a Partition based Trigger.
 */
public class PartitionTriggeringInfo implements TriggeringInfo {
  private final Type type;
  private final String datasetNamespace;
  private final String datasetName;
  private final int expectedNumPartitions;
  private final int actualNumPartitions;

  public PartitionTriggeringInfo(String datasetNamespace, String datasetName,
                                 int expectedNumPartitions, int actualNumPartitions) {
    this.type = Type.PARTITION;
    this.datasetNamespace = datasetNamespace;
    this.datasetName = datasetName;
    this.expectedNumPartitions = expectedNumPartitions;
    this.actualNumPartitions = actualNumPartitions;
  }

  /**
   * @return The type of the trigger
   */
  @Override
  public Type getType() {
    return type;
  }

  /**
   * @return The namespace of the dataset
   */
  public String getDatasetNamespace() {
    return datasetNamespace;
  }

  /**
   * @return The name of the dataset
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * @return The expected number of new partitions required for the trigger
   */
  public int getExpectedNumPartitions() {
    return expectedNumPartitions;
  }

  /**
   * @return The actual number of new partitions that caused the trigger
   */
  public int getActualNumPartitions() {
    return actualNumPartitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionTriggeringInfo)) {
      return false;
    }
    PartitionTriggeringInfo that = (PartitionTriggeringInfo) o;
    return getExpectedNumPartitions() == that.getExpectedNumPartitions()
      && getActualNumPartitions() == that.getActualNumPartitions()
      && getType() == that.getType()
      && Objects.equals(getDatasetNamespace(), that.getDatasetNamespace())
      && Objects.equals(getDatasetName(), that.getDatasetName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getDatasetNamespace(),
                        getDatasetName(), getExpectedNumPartitions(), getActualNumPartitions());
  }
}
