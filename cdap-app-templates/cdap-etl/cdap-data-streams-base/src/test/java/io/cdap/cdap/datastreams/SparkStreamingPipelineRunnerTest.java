/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.datastreams;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for SparkStreamingPipelineRunner
 */
public class SparkStreamingPipelineRunnerTest {

  @Test
  public void testRetry() {
    AtomicInteger attempt = new AtomicInteger(1);
    Runnable testRunnable = () -> {
      if (attempt.getAndIncrement() < 3) {
        throw new RuntimeException("test exception");
      }
    };

    DataStreamsPipelineSpec spec = DataStreamsPipelineSpec.builder(10000)
      .setBaseRetryDelayInSeconds(1)
      .setMaxRetryTimeInMins(2)
      .setMaxRetryDelayInSeconds(10)
      .setStateSpec(DataStreamsStateSpec.getBuilder(DataStreamsStateSpec.Mode.NONE).build())
      .build();
    SparkStreamingPipelineRunner runner = new SparkStreamingPipelineRunner(null, null, spec);
    Runnable runnableWithRetries = runner.getRunnableWithRetries(testRunnable);
    // Should run without throwing exception
    runnableWithRetries.run();
  }

  @Test(expected = RuntimeException.class)
  public void testRetryTimeout() {
    long startTime = System.currentTimeMillis();
    Runnable testRunnable = () -> {
      if (System.currentTimeMillis() - startTime > TimeUnit.MINUTES.toMillis(3)) {
        return;
      }
      throw new RuntimeException("test exception");

    };

    DataStreamsPipelineSpec spec = DataStreamsPipelineSpec.builder(10000)
      .setBaseRetryDelayInSeconds(1)
      .setMaxRetryTimeInMins(2)
      .setMaxRetryDelayInSeconds(10)
      .setStateSpec(DataStreamsStateSpec.getBuilder(DataStreamsStateSpec.Mode.NONE).build())
      .build();
    SparkStreamingPipelineRunner runner = new SparkStreamingPipelineRunner(null, null, spec);
    Runnable runnableWithRetries = runner.getRunnableWithRetries(testRunnable);
    runnableWithRetries.run();
  }
}
