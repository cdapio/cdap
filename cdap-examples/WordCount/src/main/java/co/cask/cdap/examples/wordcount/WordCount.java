/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.examples.wordcount;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;

/**
 * Word count sample Application.
 */
public class WordCount extends AbstractApplication {

  @Override
  public void configure() {
    setName("WordCount");
    setDescription("Example Word Count Application");
    
    // Ingest data into the Application via Streams
    addStream(new Stream("wordStream"));

    // Store processed data in Datasets
    createDataset("wordStats", Table.class);
    createDataset("wordCounts", KeyValueTable.class);
    createDataset("uniqueCount", UniqueCountTable.class);
    createDataset("wordAssocs", AssociationTable.class);
    
    // Process events in real-time using Flows
    addFlow(new WordCounter());

    // Retrieve the processed data using a Service
    addService(new RetrieveCounts());
  }
}
