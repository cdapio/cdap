/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data.file.filter;

import co.cask.cdap.data.file.ReadFilter;

/**
 * {@link co.cask.cdap.data.file.ReadFilter} for filtering expired stream events according to TTL and current time.
 */
public class TTLReadFilter extends ReadFilter {

  /**
   * Time to live.
   */
  private final long ttl;
  private long minStartTime;

  public TTLReadFilter(long ttl) {
    this.ttl = ttl;
  }

  public final long getTTL() {
    return ttl;
  }

  @Override
  public void reset() {
    minStartTime = -1L;
  }

  @Override
  public final boolean acceptTimestamp(long timestamp) {
    minStartTime = getCurrentTime() - ttl;
    return timestamp >= minStartTime;
  }

  @Override
  public final long getNextTimestampHint() {
    return minStartTime;
  }

  protected long getCurrentTime() {
    return System.currentTimeMillis();
  }
}
