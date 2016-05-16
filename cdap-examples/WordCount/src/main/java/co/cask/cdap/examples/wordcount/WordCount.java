/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;

/**
 * Word count sample Application. Includes a configuration class which can be used to pass in a configuration during
 * application deployment time.
 *
 * @see WordCount.WordCountConfig
 */
public class WordCount extends AbstractApplication<WordCount.WordCountConfig> {

  /**
   * Word Count Application's configuration class.
   */
  public static class WordCountConfig extends Config {
    private String stream;
    private String wordStatsTable;
    private String wordCountTable;
    private String uniqueCountTable;
    private String wordAssocTable;

    /**
     * Set default values for the configuration variables.
     */
    public WordCountConfig() {
      this.stream = "wordStream";
      this.wordStatsTable = "wordStats";
      this.wordCountTable = "wordCounts";
      this.uniqueCountTable = "uniqueCount";
      this.wordAssocTable = "wordAssocs";
    }

    /**
     * Used only for unit testing.
     */
    public WordCountConfig(String stream, String wordStatsTable, String wordCountTable, String uniqueCountTable,
                           String wordAssocTable) {
      this.stream = stream;
      this.wordStatsTable = wordStatsTable;
      this.wordCountTable = wordCountTable;
      this.uniqueCountTable = uniqueCountTable;
      this.wordAssocTable = wordAssocTable;
    }

    public String getStream() {
      return stream;
    }

    public String getWordStatsTable() {
      return wordStatsTable;
    }

    public String getWordCountTable() {
      return wordCountTable;
    }

    public String getUniqueCountTable() {
      return uniqueCountTable;
    }

    public String getWordAssocTable() {
      return wordAssocTable;
    }
  }

  @Override
  public void configure() {
    WordCountConfig config = getConfig();
    setName("WordCount");
    setDescription("Example word count application");

    // Ingest data into the Application via Streams
    addStream(new Stream(config.getStream()));

    // Store processed data in Datasets
    createDataset(config.getWordStatsTable(), Table.class,
                  DatasetProperties.builder().setDescription("Stats of total counts and lengths of words").build());
    createDataset(config.getWordCountTable(), KeyValueTable.class,
                  DatasetProperties.builder().setDescription("Words and corresponding counts").build());
    createDataset(config.getUniqueCountTable(), UniqueCountTable.class,
                  DatasetProperties.builder().setDescription("Total count of unique words").build());
    createDataset(config.getWordAssocTable(), AssociationTable.class,
                  DatasetProperties.builder().setDescription("Word associations table").build());

    // Process events in real-time using Flows
    addFlow(new WordCounter(config));

    // Retrieve the processed data using a Service
    addService(new RetrieveCounts(config));
  }
}
