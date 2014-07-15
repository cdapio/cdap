/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.common.logging.perf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handy util for debugging performance.
 * Usage: {@code
    public class MyClass {
      private static final long REPORT_INTERVAL = TimeUnit.SECONDS.toMillis(1);
      private Timing myMethodTiming = new Timing("myMethod", REPORT_INTERVAL);

      public void myMethod() {
        myMethodTiming.start();
        try {
          // ...
        } finally {
          myMethodTiming.end();
        }
      }
    }
 * }
 *
 * The code above will time calls and report when end() is called and it has been more than 1 second since last report.
 */
public class Timing {
  private static final Logger LOG = LoggerFactory.getLogger(Timing.class);

  private final String name;
  private final long reportInterval;

  private long startTs;
  private long lastReportedTs;
  private long currentIntervalCount;
  private long currentIntervalLatency;
  private long totalCount;
  private long totalLatency;

  private long start;

  public Timing(String name, long reportInterval) {
    this.name = name;
    this.reportInterval = reportInterval;
    this.startTs = System.currentTimeMillis();
  }

  public void start() {
    start = System.currentTimeMillis();
  }

  public void end() {
    long now = System.currentTimeMillis();
    long latency = now - start;

    totalCount += 1;
    totalLatency += latency;
    currentIntervalCount += 1;
    currentIntervalLatency += latency;

    // report if needed
    if (now > lastReportedTs + reportInterval) {
      LOG.info(name + " stats. " +
                 " total: " +
                 " {count: " + totalCount +
                 ", time since start: " + (now - startTs) +
                 ", avg latency: " + round(totalLatency / totalCount) + "}" +
                 " last interval: " +
                 " {count: " + currentIntervalCount +
                 ", time since interval start: " + (now - lastReportedTs) +
                 ", avg latency: " + round(currentIntervalLatency / currentIntervalCount) + "}");
      currentIntervalCount = 0;
      currentIntervalLatency = 0;
      lastReportedTs = now;
    }
  }

  private double round(double val) {
    return (((int) (val * 100)) / 100.0);
  }
}

