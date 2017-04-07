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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchJoinerContext;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.planner.StageInfo;

/**
 * Batch Joiner context
 */
public abstract class AbstractJoinerContext extends AbstractBatchContext implements BatchJoinerContext {
  private Integer numPartitions;
  private Class<?> joinKeyClass;
  private Class<?> joinInputRecordClass;

  protected <T extends PluginContext & DatasetContext> AbstractJoinerContext(T context,
                                                                             Metrics metrics,
                                                                             LookupProvider lookup,
                                                                             long logicalStartTime,
                                                                             Admin admin,
                                                                             StageInfo stageInfo,
                                                                             BasicArguments arguments) {
    super(context, metrics, lookup, logicalStartTime, admin, stageInfo, arguments);
  }

  @Override
  public void setNumPartitions(int numPartitions) {
    if (numPartitions < 1) {
      throw new IllegalArgumentException(String.format(
        "Invalid value for numPartitions %d. It must be a positive integer.", numPartitions));
    }
    this.numPartitions = numPartitions;
  }

  public Integer getNumPartitions() {
    return numPartitions;
  }

  @Override
  public void setJoinKeyClass(Class<?> joinKeyClass) {
    this.joinKeyClass = joinKeyClass;
  }

  @Override
  public void setJoinInputRecordClass(Class<?> joinInputRecordClass) {
    this.joinInputRecordClass = joinInputRecordClass;
  }

  public Class<?> getJoinKeyClass() {
    return joinKeyClass;
  }

  public Class<?> getJoinInputRecordClass() {
    return joinInputRecordClass;
  }
}
