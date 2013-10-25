/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
