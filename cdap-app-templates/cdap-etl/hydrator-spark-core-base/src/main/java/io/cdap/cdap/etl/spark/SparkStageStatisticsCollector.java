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

package io.cdap.cdap.etl.spark;

import io.cdap.cdap.etl.common.StageStatisticsCollector;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;

/**
 * Implementation of {@link StageStatisticsCollector} for batch spark pipelines.
 */
public class SparkStageStatisticsCollector implements StageStatisticsCollector, Serializable {
  private static final long serialVersionUID = -7897960584858589315L;
  private final LongAccumulator inputRecordCounter;
  private final LongAccumulator outputRecordCounter;
  private final LongAccumulator errorRecordCounter;

  public SparkStageStatisticsCollector(JavaSparkContext jsc) {
    this.inputRecordCounter = jsc.sc().longAccumulator();
    this.outputRecordCounter = jsc.sc().longAccumulator();
    this.errorRecordCounter = jsc.sc().longAccumulator();
  }

  @Override
  public void incrementInputRecordCount() {
    inputRecordCounter.add(1);
  }

  @Override
  public void incrementOutputRecordCount() {
    outputRecordCounter.add(1);
  }

  @Override
  public void incrementErrorRecordCount() {
    errorRecordCounter.add(1);
  }

  public long getInputRecordCount() {
    return inputRecordCounter.value();
  }

  public long getOutputRecordCount() {
    return outputRecordCounter.value();
  }

  public long getErrorRecordCount() {
    return errorRecordCounter.value();
  }
}
