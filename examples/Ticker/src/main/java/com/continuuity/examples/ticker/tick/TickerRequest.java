/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.ticker.tick;

/**
 * Representation of a request to get data for a specific ticker for some time range.
 */
public final class TickerRequest {

  private final String ticker;
  private final long startTs;
  private final long endTs;

  public TickerRequest(String ticker, long startTs, long endTs) {
    this.ticker = ticker;
    this.startTs = startTs;
    this.endTs = endTs;
  }

  public String getTicker() {
    return ticker;
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }
}
