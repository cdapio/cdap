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
