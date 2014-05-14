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
