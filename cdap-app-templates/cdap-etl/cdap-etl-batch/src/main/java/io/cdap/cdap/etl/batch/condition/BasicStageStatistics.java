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

import co.cask.cdap.etl.api.condition.StageStatistics;

/**
 * Default implementation of the {@link StageStatistics}.
 */
public class BasicStageStatistics implements StageStatistics {
  private final long numOfInputRecords;
  private final long numOfOutputRecords;
  private final long numOfErrorRecords;

  public BasicStageStatistics(long numOfInputRecords, long numOfOutputRecords, long numOfErrorRecords) {
    this.numOfInputRecords = numOfInputRecords;
    this.numOfOutputRecords = numOfOutputRecords;
    this.numOfErrorRecords = numOfErrorRecords;
  }

  @Override
  public long getInputRecordsCount() {
    return numOfInputRecords;
  }

  @Override
  public long getOutputRecordsCount() {
    return numOfOutputRecords;
  }

  @Override
  public long getErrorRecordsCount() {
    return numOfErrorRecords;
  }
}
