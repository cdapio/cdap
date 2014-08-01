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
package com.continuuity.data.file.filter;

import com.continuuity.data.file.ReadFilter;

/**
 * {@link com.continuuity.data.file.ReadFilter} for filtering expired stream events according to TTL and current time.
 */
public class TTLReadFilter extends ReadFilter {

  /**
   * Time to live.
   */
  private long ttl;

  public TTLReadFilter(long ttl) {
    this.ttl = ttl;
  }

  public long getTTL() {
    return ttl;
  }

  @Override
  public boolean acceptTimestamp(long timestamp) {
    return getCurrentTime() - timestamp <= ttl;
  }

  protected long getCurrentTime() {
    return System.currentTimeMillis();
  }
}
