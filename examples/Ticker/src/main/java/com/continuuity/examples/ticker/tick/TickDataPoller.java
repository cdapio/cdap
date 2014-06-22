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

import com.continuuity.api.annotation.Tick;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.dataset.lib.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Periodically sends out requests to pull new data for the master set of tickers.
 */
public class TickDataPoller extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(TickDataPoller.class);

  @UseDataSet("tickerSet")
  private KeyValueTable tickerSet;

  private OutputEmitter<TickerRequest> output;

  @Tick(delay = 1L, unit = TimeUnit.MINUTES)
  public void generate() throws InterruptedException {
    long now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.debug("poller woke up, reading ticker set for updates...");
    for (Split split : tickerSet.getSplits()) {
      SplitReader<byte[], byte[]> splitReader = tickerSet.createSplitReader(split);
      splitReader.initialize(split);
      do {
        if (splitReader.getCurrentKey() != null) {
          String ticker = Bytes.toString(splitReader.getCurrentKey());
          long start = now - 61;
          long end = now + 1;
          output.emit(new TickerRequest(ticker, start, end));
        }
      } while (splitReader.nextKeyValue());
      splitReader.close();
    }
  }
}
