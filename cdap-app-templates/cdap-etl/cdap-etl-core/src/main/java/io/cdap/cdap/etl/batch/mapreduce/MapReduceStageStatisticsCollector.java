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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.StageStatisticsCollector;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Implementation of the {@link StageStatisticsCollector} for the pipeline which executes as a MapReduce program.
 */
public class MapReduceStageStatisticsCollector implements StageStatisticsCollector {
  private final String inputRecordCountKey;
  private final String outputRecordCountKey;
  private final String errorRecordCountKey;
  private final TaskAttemptContext context;

  public MapReduceStageStatisticsCollector(String stageName, TaskAttemptContext context) {
    this.inputRecordCountKey = stageName + "." + Constants.StageStatistics.INPUT_RECORDS;
    this.outputRecordCountKey = stageName + "." + Constants.StageStatistics.OUTPUT_RECORDS;
    this.errorRecordCountKey = stageName + "." + Constants.StageStatistics.ERROR_RECORDS;
    this.context = context;
  }

  @Override
  public void incrementInputRecordCount() {
    context.getCounter(Constants.StageStatistics.PREFIX, inputRecordCountKey).increment(1);
  }

  @Override
  public void incrementOutputRecordCount() {
    context.getCounter(Constants.StageStatistics.PREFIX, outputRecordCountKey).increment(1);
  }

  @Override
  public void incrementErrorRecordCount() {
    context.getCounter(Constants.StageStatistics.PREFIX, errorRecordCountKey).increment(1);
  }
}
