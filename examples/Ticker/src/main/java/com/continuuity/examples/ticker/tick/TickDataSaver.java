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

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.dataset.TimeseriesTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives tick data that has been aggregated at the second level, and stores the data to the timeseries table.
 */
public class TickDataSaver extends AbstractFlowlet {

  private static final Logger LOG = LoggerFactory.getLogger(TickDataSaver.class);

  @UseDataSet("tickTimeseries")
  private SimpleTimeseriesTable tickTs;
  private Metrics metrics;

  private static final byte[] MIN_TAG = Bytes.toBytes("min");
  private static final byte[] MAX_TAG = Bytes.toBytes("max");
  private static final byte[] AVG_TAG = Bytes.toBytes("avg");
  private static final byte[] QUANTITY_TAG = Bytes.toBytes("quantity");

  @ProcessInput
  public void process(TickerDataSecond dataPoint) {
    if (dataPoint == null) {
      LOG.error("got a null dataPoint for some reason");
      return;
    }
    LOG.debug("saving data:" + dataPoint.toString());
    long ts = dataPoint.getTimestamp();
    byte[] ticker = Bytes.toBytes(dataPoint.getTicker());
    byte[] exchange = Bytes.toBytes(dataPoint.getExchange().name());
    metrics.count("ticker.saves", 1);

    // row key is the ticker symbol.  tags include avg, min, max, and exchange so each can be queried separately.
    tickTs.write(new TimeseriesTable.Entry(ticker, Bytes.toBytes(dataPoint.getAvgPrice()), ts, AVG_TAG, exchange));
    tickTs.write(new TimeseriesTable.Entry(ticker, Bytes.toBytes(dataPoint.getMinPrice()), ts, MIN_TAG, exchange));
    tickTs.write(new TimeseriesTable.Entry(ticker, Bytes.toBytes(dataPoint.getMaxPrice()), ts, MAX_TAG, exchange));
    tickTs.write(new TimeseriesTable.Entry(ticker, Bytes.toBytes(dataPoint.getQuantity()), ts, QUANTITY_TAG, exchange));
  }
}
