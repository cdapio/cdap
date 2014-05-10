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

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.examples.ticker.data.MultiIndexedTable;
import com.continuuity.examples.ticker.order.OrderDataSaver;
import com.continuuity.examples.ticker.query.Orders;
import com.continuuity.examples.ticker.query.Summary;
import com.continuuity.examples.ticker.query.Timeseries;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * The Ticker App consists of 2 Flows
 */
public class TickerApp implements Application {
  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}.
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    Set<byte[]> doNotIndexFields = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
    doNotIndexFields.add(OrderDataSaver.PAYLOAD_COL);

    return ApplicationSpecification.Builder.with()
      .setName("Ticker")
      .setDescription("Application for storing and querying stock tick data")
      .withStreams()
        .add(new Stream("tickers"))
        .add(new Stream("orders"))
      .withDataSets()
        .add(new SimpleTimeseriesTable("tickTimeseries", 60 * 60))
        .add(new KeyValueTable("tickerSet"))
        .add(new MultiIndexedTable("orderIndex", OrderDataSaver.TIMESTAMP_COL, doNotIndexFields))
      .withFlows()
        .add(new TickStreamFlow())
        .add(new OrderStreamFlow())
      .withProcedures()
        .add(new Timeseries())
        .add(new Orders())
        .add(new Summary())
      .noMapReduce()
      .noWorkflow()
      .build();
  }
}

