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
package com.continuuity.examples.simplewriteandread;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;

/**
 * The SimpleWriteAndRead application uses one dataset, one flow, three flowlets, and one stream to demonstrate
 * how to write to and read from a dataset. 
 */
public class SimpleWriteAndRead implements Application {

  public static final String TABLE_NAME = "writeAndRead";

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("SimpleWriteAndRead")
      .setDescription("Flow that writes key=value then reads back the key")
      .withStreams()
        .add(new Stream("keyValues"))
      .withDataSets()
        .add(new KeyValueTable(TABLE_NAME))
      .withFlows()
        .add(new SimpleWriteAndReadFlow())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }
}
