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

package co.cask.cdap.etl.spark;

import co.cask.cdap.etl.common.StageStatisticsCollector;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Implementation of {@link StageStatisticsCollector} for batch spark pipelines.
 */
public class SparkStageStatisticsCollector implements StageStatisticsCollector, Serializable {
  private static final long serialVersionUID = -7897960584858589314L;
  private final Accumulator<Double> inputRecordCounter;
  private final Accumulator<Double> outputRecordCounter;
  private final Accumulator<Double> errorRecordCounter;

  public SparkStageStatisticsCollector(JavaSparkContext jsc) {
    this.inputRecordCounter = jsc.accumulator(0.0);
    this.outputRecordCounter = jsc.accumulator(0.0);
    this.errorRecordCounter = jsc.accumulator(0.0);
  }

  @Override
  public void incrementInputRecordCount() {
    inputRecordCounter.add(1.0);
  }

  @Override
  public void incrementOutputRecordCount() {
    outputRecordCounter.add(1.0);
  }

  @Override
  public void incrementErrorRecordCount() {
    errorRecordCounter.add(1.0);
  }

  public long getInputRecordCount() {
    return inputRecordCounter.value().longValue();
  }

  public long getOutputRecordCount() {
    return outputRecordCounter.value().longValue();
  }

  public long getErrorRecordCount() {
    return errorRecordCounter.value().longValue();
  }
}
