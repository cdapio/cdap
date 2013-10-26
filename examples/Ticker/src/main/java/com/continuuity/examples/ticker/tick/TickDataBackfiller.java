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
import com.continuuity.api.annotation.Property;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
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
 * Receives a csv of ticker symbols from its input stream, stores new symbols into the dataset containing
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

      // backfill data
      long now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      for (String ticker : tickers) {
        // if the ticker is already in our set, no need to re-process
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
