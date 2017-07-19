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

package co.cask.cdap.etl.batch.condition;

import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.condition.StageStatistics;
import co.cask.cdap.etl.common.Constants;

/**
 * Default implementation of the {@link StageStatistics}.
 */
public class BasicStageStatistics implements StageStatistics {
  private final WorkflowToken token;
  private final String stageName;

  public BasicStageStatistics(WorkflowToken token, String stageName) {
    this.token = token;
    this.stageName = stageName;
  }

  @Override
  public long getInputRecordsCount() {
    return getValue(Constants.StageStatistics.INPUT_RECORDS);
  }

  @Override
  public long getOutputRecordsCount() {
    return getValue(Constants.StageStatistics.OUTPUT_RECORDS);
  }

  @Override
  public long getErrorRecordsCount() {
    return getValue(Constants.StageStatistics.ERROR_RECORDS);
  }

  private long getValue(String property) {
    String stageProperty = stageName + "." + property;
    Value value = token.get(stageProperty);
    if (value != null) {
      return value.getAsLong();
    }
    return 0;
  }
}
