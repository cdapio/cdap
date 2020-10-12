/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.etl.spark.AbstractSparkCounter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.OutputMetrics;
import org.apache.spark.executor.TaskMetrics;

import java.util.Optional;

/**
 * A {@link Counter} that records bytes written values to Spark metrics.
 */
public class SparkBytesWrittenCounter extends AbstractSparkCounter {

  public SparkBytesWrittenCounter() {
    super("bytes.written", "Bytes Written");
  }

  @Override
  public long getValue() {
    return getOutputMetrics().map(OutputMetrics::bytesWritten).orElse(0L);
  }

  @Override
  public void setValue(long value) {
    getOutputMetrics().ifPresent(m -> m.setBytesWritten(value));
  }

  @Override
  public void increment(long incr) {
    long currentValue = getValue();
    getOutputMetrics().ifPresent(m -> m.setBytesWritten(currentValue + incr));
  }

  private Optional<OutputMetrics> getOutputMetrics() {
    return Optional.ofNullable(TaskContext.get())
      .map(TaskContext::taskMetrics)
      .map(TaskMetrics::outputMetrics)
      .flatMap(o -> Optional.ofNullable(o.getOrElse(null)));
  }
}
