/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.gateway.apps.wordcount;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.dataset.lib.KeyValueTable;
import com.continuuity.api.dataset.table.Table;

/**
 * Word count sample application.
 */
public class WCount extends AbstractApplication {
  public static void main(String[] args) {
    // Main method should be defined for Application to get deployed with Eclipse IDE plugin. DO NOT REMOVE IT
  }

  @Override
  public void configure() {
    setName("WCount");
    setDescription("another Word Count Application");
    addStream(new Stream("words"));
    createDataset("stats", Table.class);
    createDataset("wordCounts", KeyValueTable.class);
    createDataset("jobConfig", KeyValueTable.class);
    createDataset("uniqueCount", UniqueCountTable.class);
    createDataset("wordAssocs", AssociationTable.class);
    createDataset("jobConfig", KeyValueTable.class);
    addFlow(new WCounter());
    addFlow(new WordCounter());
    addProcedure(new RCounts());
    addMapReduce(new ClassicWordCount());
  }
}
