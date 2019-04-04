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

package io.cdap.cdap.etl.batch;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.etl.api.batch.BatchAggregatorContext;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

/**
 * Batch Aggregator Context.
 */
public class DefaultAggregatorContext extends AbstractBatchContext implements BatchAggregatorContext {
  private Integer numPartitions;
  private Class<?> groupKeyClass;
  private Class<?> groupValueClass;

  public DefaultAggregatorContext(PipelineRuntime pipelineRuntime, StageSpec stageSpec,
                                  DatasetContext datasetContext, Admin admin) {
    super(pipelineRuntime, stageSpec, datasetContext, admin);
  }

  @Override
  public void setNumPartitions(int numPartitions) {
    if (numPartitions < 1) {
      throw new IllegalArgumentException(String.format(
        "Invalid value for numPartitions %d. It must be a positive integer.", numPartitions));
    }
    this.numPartitions = numPartitions;
  }

  @Override
  public void setGroupKeyClass(Class<?> groupKeyClass) {
    this.groupKeyClass = groupKeyClass;
  }

  @Override
  public void setGroupValueClass(Class<?> groupValueClass) {
    this.groupValueClass = groupValueClass;
  }

  public Integer getNumPartitions() {
    return numPartitions;
  }

  public Class<?> getGroupKeyClass() {
    return groupKeyClass;
  }

  public Class<?> getGroupValueClass() {
    return groupValueClass;
  }
}
