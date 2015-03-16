/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.core;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;

/**
 * This is a simple HelloWorld example that uses one stream, one dataset, one flow and one service.
 * <uL>
 *   <li>A stream to send names to.</li>
 *   <li>A flow with a single flowlet that reads the stream and stores each name in a KeyValueTable</li>
 *   <li>A service that reads the name from the KeyValueTable and responds with 'Hello [Name]!'</li>
 * </uL>
 */
public class ETLDriverApplication extends AbstractApplication {

  @Override
  public void configure() {
    setName("HelloWorld");
    setDescription("A Hello World program for the Cask Data Application Platform");
    addWorker(new RealtimeAdapterDriver());
    addMapReduce(new BatchAdapterDriver());
    addStream(new Stream("batchStream"));
    createDataset("tweetTable", KeyValueTable.class);
    createDataset("stateStore", KeyValueTable.class);
    createDataset("myTable", Table.class);
    createDataset("KVTableBatchSink", KeyValueTable.class);
    createDataset("KVTableBatchSource", KeyValueTable.class);
  }
}
