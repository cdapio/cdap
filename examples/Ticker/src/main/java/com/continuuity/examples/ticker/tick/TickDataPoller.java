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

import com.continuuity.api.annotation.Tick;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.KeyValueTable;
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
