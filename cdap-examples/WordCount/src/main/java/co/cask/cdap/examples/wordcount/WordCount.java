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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;

/**
 * Word count sample Application. Uses a configuration class which can be used to pass in a configuration during
 * application creation time.
 *
 * @see {@link WordCountConfig}
 */
public class WordCount extends AbstractApplication<WordCount.WordCountConfig> {

  /**
   * Word Count Application's configuration class.
   */
  public static class WordCountConfig extends Config {
    private String stream;
    private String wsTable;
    private String wcTable;
    private String ucTable;
    private String waTable;

    public WordCountConfig() {
      this.stream = "wordStream";
      this.wsTable = "wordStats";
      this.wcTable = "wordCounts";
      this.ucTable = "unqiueCount";
      this.waTable = "wordAssocs";
    }

    public WordCountConfig(String stream, String wsTable, String wcTable, String ucTable, String waTable) {
      this.stream = stream;
      this.wsTable = wsTable;
      this.wcTable = wcTable;
      this.ucTable = ucTable;
      this.waTable = waTable;
    }

    public String getStream() {
      return stream;
    }

    public String getWsTable() {
      return wsTable;
    }

    public String getWcTable() {
      return wcTable;
    }

    public String getUcTable() {
      return ucTable;
    }

    public String getWaTable() {
      return waTable;
    }
  }

  @Override
  public void configure() {
    WordCountConfig config = getConfig();
    setName("WordCount");
    setDescription("Example Word Count Application");

    // Ingest data into the Application via Streams
    addStream(new Stream(config.getStream()));

    // Store processed data in Datasets
    createDataset(config.getWsTable(), Table.class);
    createDataset(config.getWcTable(), KeyValueTable.class);
    createDataset(config.getUcTable(), UniqueCountTable.class);
    createDataset(config.getWaTable(), AssociationTable.class);

    // Process events in real-time using Flows
    addFlow(new WordCounter(config));

    // Retrieve the processed data using a Service
    addService(new RetrieveCounts(config));
  }
}
