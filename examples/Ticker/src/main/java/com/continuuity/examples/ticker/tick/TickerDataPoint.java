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
 * A single data point representing a trade that occurred.
 */
public final class TickerDataPoint {

  private final String ticker;
  private final Exchange exchange;
  private final long timestamp;
  private final int quantity;
  private final float price;

  public TickerDataPoint(String ticker, Exchange exchange, long timestamp, int quantity, float price) {
    this.ticker = ticker;
    this.exchange = exchange;
    this.timestamp = timestamp;
    this.quantity = quantity;
    this.price = price;
  }

  public String getTicker() {
    return ticker;
  }

  public Exchange getExchange() {
    return exchange;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getQuantity() {
    return quantity;
  }

  public float getPrice() {
    return price;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof TickerDataPoint)) {
      return false;
    }
    TickerDataPoint other = (TickerDataPoint) o;
    return ticker.equals(other.ticker) &&
      exchange == other.exchange &&
      timestamp == other.timestamp &&
      quantity == other.quantity &&
      price == other.price;
  }
}
