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
 * An aggregate of one or more {@link TickerDataPoint} that happened in the same second.
 */
public final class TickerDataSecond {

  private final String ticker;
  private final Exchange exchange;
  private final long timestamp;
  private int quantity;
  private float minPrice;
  private float maxPrice;
  private float totalPrice;

  public TickerDataSecond(String ticker, Exchange exchange, long timestamp) {
    this.ticker = ticker;
    this.exchange = exchange;
    this.timestamp = timestamp;
    this.minPrice = Float.MAX_VALUE;
    this.maxPrice = Float.MIN_VALUE;
    this.totalPrice = 0;
    this.quantity = 0;
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

  public float getMinPrice() {
    return minPrice;
  }

  public float getMaxPrice() {
    return maxPrice;
  }

  public float getAvgPrice() {
    return totalPrice / (float) quantity;
  }

  public void addPoint(TickerDataPoint point) {
    float price = point.getPrice();
    totalPrice += point.getQuantity() * price;
    if (price < minPrice) {
      minPrice = price;
    }
    if (price > maxPrice) {
      maxPrice = price;
    }
    quantity += point.getQuantity();
  }

  @Override
  public String toString() {
    StringBuilder out = new StringBuilder();
    out.append("{ticker=");
    out.append(ticker);
    out.append(", exchange=");
    out.append(exchange.name());
    out.append(", timestamp=");
    out.append(timestamp);
    out.append(", quantity=");
    out.append(quantity);
    out.append(", minPrice=");
    out.append(minPrice);
    out.append(", maxPrice=");
    out.append(maxPrice);
    out.append(", avgPrice=");
    out.append(getAvgPrice());
    out.append("}");
    return out.toString();
  }
}
