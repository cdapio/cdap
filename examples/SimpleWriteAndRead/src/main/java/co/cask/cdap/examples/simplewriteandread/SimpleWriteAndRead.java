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
package co.cask.cdap.examples.simplewriteandread;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

/**
 * The SimpleWriteAndRead application uses one dataset, one flow, three flowlets, and one stream to demonstrate
 * how to write to and read from a dataset. 
 */
public class SimpleWriteAndRead extends AbstractApplication {

  public static final String TABLE_NAME = "writeAndRead";

  @Override
  public void configure() {
    setName("SimpleWriteAndRead");
    setDescription("Flow that writes key=value then reads back the key");
    addStream(new Stream("keyValues"));
    createDataset(TABLE_NAME, KeyValueTable.class);
    addFlow(new SimpleWriteAndReadFlow());
  }
}
