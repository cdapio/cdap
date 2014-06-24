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

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.lib.TimeseriesTable;
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
  private TimeseriesTable tickTs;
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

    // Row key is the ticker symbol; tags include avg, min, max, and exchange so each can be queried separately.
    tickTs.write(new TimeseriesTable.Entry(ticker, Bytes.toBytes(dataPoint.getAvgPrice()), ts, AVG_TAG, exchange));
    tickTs.write(new TimeseriesTable.Entry(ticker, Bytes.toBytes(dataPoint.getMinPrice()), ts, MIN_TAG, exchange));
    tickTs.write(new TimeseriesTable.Entry(ticker, Bytes.toBytes(dataPoint.getMaxPrice()), ts, MAX_TAG, exchange));
    tickTs.write(new TimeseriesTable.Entry(ticker, Bytes.toBytes(dataPoint.getQuantity()), ts, QUANTITY_TAG, exchange));
  }
}
