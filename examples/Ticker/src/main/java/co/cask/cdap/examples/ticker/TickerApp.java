/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.examples.ticker;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.TimeseriesTables;
import co.cask.cdap.examples.ticker.data.MultiIndexedTable;
import co.cask.cdap.examples.ticker.order.OrderDataSaver;
import co.cask.cdap.examples.ticker.query.Orders;
import co.cask.cdap.examples.ticker.query.Summary;
import co.cask.cdap.examples.ticker.query.Timeseries;
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

