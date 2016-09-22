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

package co.cask.cdap.etl.spark.batch;

import java.util.Map;

/**
 * Stores all the information of {@link SparkBatchSinkFactory} and stagePartitions
 */
public class SparkBatchSourceSinkFactoryInfo {
  private final SparkBatchSourceFactory sparkBatchSourceFactory;
  private final SparkBatchSinkFactory sparkBatchSinkFactory;
  private final Map<String, Integer> stagePartitions;

  public SparkBatchSourceSinkFactoryInfo(SparkBatchSourceFactory sparkBatchSourceFactory,
                                         SparkBatchSinkFactory sparkBatchSinkFactory,
                                         Map<String, Integer> stagePartitions) {
    this.sparkBatchSourceFactory = sparkBatchSourceFactory;
    this.sparkBatchSinkFactory = sparkBatchSinkFactory;
    this.stagePartitions = stagePartitions;
  }

  public SparkBatchSourceFactory getSparkBatchSourceFactory() {
    return sparkBatchSourceFactory;
  }

  public SparkBatchSinkFactory getSparkBatchSinkFactory() {
    return sparkBatchSinkFactory;
  }

  public Map<String, Integer> getStagePartitions() {
    return stagePartitions;
  }
}
