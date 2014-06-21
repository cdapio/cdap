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
import com.continuuity.api.annotation.Property;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.lib.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.metrics.Metrics;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Receives a CSV of ticker symbols from its input stream, stores new symbols into the dataset containing
 * the master set of symbols, and sends off requests to pull in data for those new symbols for the past X
 * days, where X can be specified in the flow specification.
 */
public class TickDataBackfiller extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(TickDataBackfiller.class);
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd", new Locale("no", "NO"));

  @UseDataSet("tickerSet")
  private KeyValueTable tickerSet;

  @Property
  private final int backfill;

  private OutputEmitter<TickerRequest> output;
  private Metrics metrics;

  public TickDataBackfiller(int backfill) {
    this.backfill = backfill;
  }

  @ProcessInput
  public void process(StreamEvent event) throws CharacterCodingException {
    ByteBuffer buf = event.getBody();
    if (buf != null) {
      String tickerCSV = Charsets.UTF_8.newDecoder().decode(buf).toString();
      String[] tickers = tickerCSV.split(",");

      // Backfill data
      long now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      for (String ticker : tickers) {
        
        // If the ticker is already in our set, no need to re-process
        if (tickerSet.read(Bytes.toBytes(ticker)) != null) {
          LOG.info("already seen ticker {}, skipping backfill for it", ticker);
          metrics.count("ticker.repeats", 1);
          continue;
        }

        LOG.debug("sending backfill requests for {} for {} days", ticker, backfill);
        for (int i = 0; i < backfill; i++) {
          long end = now - 86400 * i;
          long start = end - 86400;
          output.emit(new TickerRequest(ticker, start, end));
        }
        LOG.debug("writing ticker {} to tickerSet", ticker);
        tickerSet.write(ticker, String.valueOf(now));
      }
    } else {
      metrics.count("events.nullcount", 1);
    }
  }
}
