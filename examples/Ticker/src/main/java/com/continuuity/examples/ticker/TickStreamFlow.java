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

package com.continuuity.examples.ticker;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.examples.ticker.tick.TickDataBackfiller;
import com.continuuity.examples.ticker.tick.TickDataParser;
import com.continuuity.examples.ticker.tick.TickDataPoller;
import com.continuuity.examples.ticker.tick.TickDataSaver;

/**
 *
 */
public class TickStreamFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("TickTimeseriesFlow")
      .setDescription("Flow for ingesting tick data")
      .withFlowlets()
        .add("Backfill", new TickDataBackfiller(5))
        .add("Saver", new TickDataSaver())
        .add("Poller", new TickDataPoller())
        .add("Parser", new TickDataParser())
      .connect()
        .fromStream("tickers").to("Backfill")
        .from("Backfill").to("Parser")
        .from("Poller").to("Parser")
        .from("Parser").to("Saver")
      .build();
  }
}