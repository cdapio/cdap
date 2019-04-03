/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.api.schedule.PartitionTriggerInfo;

import java.io.Serializable;

/**
 * The dataset partition trigger information to be passed to the triggered program.
 */
public class DefaultPartitionTriggerInfo extends AbstractTriggerInfo implements PartitionTriggerInfo, Serializable {
  private final String datasetNamespace;
  private final String datasetName;
  private final int expectedNumPartitions;
  private final int actualNumPartitions;

  public DefaultPartitionTriggerInfo(String datasetNamespace, String datasetName,
                                     int expectedNumPartitions, int actualNumPartitions) {
    super(Type.PARTITION);
    this.datasetNamespace = datasetNamespace;
    this.datasetName = datasetName;
    this.expectedNumPartitions = expectedNumPartitions;
    this.actualNumPartitions = actualNumPartitions;
  }

  @Override
  public String getDatasetNamespace() {
    return datasetNamespace;
  }

  @Override
  public String getDatasetName() {
    return datasetName;
  }

  @Override
  public int getExpectedNumPartitions() {
    return expectedNumPartitions;
  }

  @Override
  public int getActualNumPartitions() {
    return actualNumPartitions;
  }
}
