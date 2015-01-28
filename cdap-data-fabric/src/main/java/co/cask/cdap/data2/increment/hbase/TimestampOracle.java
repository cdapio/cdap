/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.increment.hbase;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class that allows HBase coprocessors to interact with unique timestamps.
 */
public class TimestampOracle {
  private AtomicLong lastTimestamp = new AtomicLong();

  /**
   * Returns the current wall clock time in milliseconds, multiplied by the required precision.
   */
  public long currentTime() {
    // We want to align tx ids with current time. We assume that tx ids are sequential, but not less than
    // System.currentTimeMillis() * MAX_TX_PER_MS.
    return System.currentTimeMillis() * IncrementHandlerState.MAX_TS_PER_MS;
  }


  /**
   * Returns a timestamp value unique within the scope of this {@code TimestampOracle} instance.  For usage
   * by HBase {@code RegionObserver} coprocessors, this normally means unique within a given region.
   */
  public long getUniqueTimestamp() {
    long lastTs;
    long nextTs;
    do {
      lastTs = lastTimestamp.get();
      nextTs = Math.max(lastTs + 1, currentTime());
    } while (!lastTimestamp.compareAndSet(lastTs, nextTs));
    return nextTs;
  }
}
