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

package co.cask.cdap.etl.common.plugin;

import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.common.Constants;
import com.google.common.base.Stopwatch;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Times how long it takes to call a callable, and emits it as a metric.
 */
public class TimingCaller extends Caller {
  private final Caller delegate;
  private final StageMetrics stageMetrics;
  private final Stopwatch stopwatch;
  private long minTime;
  private long maxTime;
  private long numValues;
  private double mean;
  private double m2;

  private TimingCaller(Caller delegate, StageMetrics stageMetrics) {
    this.delegate = delegate;
    this.stageMetrics = stageMetrics;
    this.stopwatch = new Stopwatch();
    this.minTime = Long.MAX_VALUE;
    this.maxTime = Long.MIN_VALUE;
    this.numValues = 0;
    this.mean = 0d;
    this.m2 = 0d;
  }

  @Override
  public <T> T call(Callable<T> callable, CallArgs args) throws Exception {
    if (!args.shouldTrackTime()) {
      return delegate.call(callable, args);
    }

    stopwatch.start();
    try {
      return delegate.call(callable, args);
    } finally {
      emitTimeMetrics(stopwatch.elapsedTime(TimeUnit.MICROSECONDS));
      stopwatch.reset();
    }
  }

  private void emitTimeMetrics(long micros) {
    maxTime = maxTime < micros ? micros : maxTime;
    minTime = minTime > micros ? micros : minTime;
    // this shouldn't normally happen, it means ~35 minutes were spent in a method call
    while (micros > Integer.MAX_VALUE) {
      stageMetrics.count(Constants.Metrics.TOTAL_TIME, Integer.MAX_VALUE);
      micros -= Integer.MAX_VALUE;
    }
    stageMetrics.count(Constants.Metrics.TOTAL_TIME, (int) micros);
    stageMetrics.gauge(Constants.Metrics.MAX_TIME, maxTime);
    stageMetrics.gauge(Constants.Metrics.MIN_TIME, minTime);

    // see https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
    numValues++;
    double delta = micros - mean;
    mean += delta / numValues;
    double delta2 = micros - mean;
    m2 += delta * delta2;
    double stddev = Math.sqrt(m2 / numValues);

    stageMetrics.gauge(Constants.Metrics.AVG_TIME, (long) mean);
    stageMetrics.gauge(Constants.Metrics.STD_DEV_TIME, (long) stddev);
  }

  public static Caller wrap(Caller delegate, StageMetrics stageMetrics) {
    return new TimingCaller(delegate, stageMetrics);
  }
}
