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

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.dataset.lib.KeyValueTable;
import com.continuuity.api.dataset.lib.TimeseriesTables;
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
public class TickerApp extends AbstractApplication {
  @Override
  public void configure() {
    Set<byte[]> doNotIndexFields = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
    doNotIndexFields.add(OrderDataSaver.PAYLOAD_COL);

    setName("Ticker");
    setDescription("Application for storing and querying stock tick data");
    addStream(new Stream("tickers"));
    addStream(new Stream("orders"));
    TimeseriesTables.createTable(getConfigurer(), "tickTimeseries", 60 * 60);
    createDataset("tickerSet", KeyValueTable.class);
    createDataset("orderIndex", MultiIndexedTable.class,
                  MultiIndexedTable.properties(OrderDataSaver.TIMESTAMP_COL, doNotIndexFields));
    addFlow(new TickStreamFlow());
    addFlow(new OrderStreamFlow());
    addProcedure(new Timeseries());
    addProcedure(new Orders());
    addProcedure(new Summary());
  }
}

