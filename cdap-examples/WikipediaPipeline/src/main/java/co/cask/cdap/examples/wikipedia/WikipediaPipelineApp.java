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

package co.cask.cdap.examples.wikipedia;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;

import javax.annotation.Nullable;

/**
 * App to demonstrate a data pipeline that processes Wikipedia data using a CDAP Workflow.
 */
public class WikipediaPipelineApp extends AbstractApplication<WikipediaPipelineApp.WikipediaAppConfig> {
  static final String PAGE_TITLES_STREAM = "pageTitleStream";
  static final String RAW_WIKIPEDIA_STREAM = "wikiStream";
  static final String PAGE_TITLES_DATASET = "pages";
  static final String RAW_WIKIPEDIA_DATASET = "wikidata";
  static final String NORMALIZED_WIKIPEDIA_DATASET = "normalized";
  static final String SPARK_CLUSTERING_OUTPUT_DATASET = "clustering";
  static final String MAPREDUCE_TOPN_OUTPUT = "topn";
  static final String LIKES_TO_DATASET_MR_NAME = "LikesToDataset";
  static final String WIKIPEDIA_TO_DATASET_MR_NAME = "WikiDataToDataset";

  @Override
  public void configure() {
    addStream(new Stream(PAGE_TITLES_STREAM));
    addStream(new Stream(RAW_WIKIPEDIA_STREAM));
    addMapReduce(new StreamToDataset(LIKES_TO_DATASET_MR_NAME));
    addMapReduce(new StreamToDataset(WIKIPEDIA_TO_DATASET_MR_NAME));
    addMapReduce(new WikipediaDataDownloader());
    addMapReduce(new WikiContentValidatorAndNormalizer());
    addMapReduce(new TopNMapReduce());
    addSpark(new SparkWikipediaClustering(getConfig()));
    createDataset(PAGE_TITLES_DATASET, KeyValueTable.class,
                  DatasetProperties.builder().setDescription("Page titles dataset").build());
    createDataset(RAW_WIKIPEDIA_DATASET, KeyValueTable.class,
                  DatasetProperties.builder().setDescription("Raw Wikipedia dataset").build());
    createDataset(NORMALIZED_WIKIPEDIA_DATASET, KeyValueTable.class,
                  DatasetProperties.builder().setDescription("Normalized Wikipedia dataset").build());
    createDataset(SPARK_CLUSTERING_OUTPUT_DATASET, Table.class,
                  DatasetProperties.builder().setDescription("Spark clustering output dataset").build());
    createDataset(MAPREDUCE_TOPN_OUTPUT, KeyValueTable.class,
                  DatasetProperties.builder().setDescription("MapReduce top-'N'-words output dataset").build());
    addWorkflow(new WikipediaPipelineWorkflow(getConfig()));
    addService(new WikipediaService());
  }

  /**
   * Config for Wikipedia App.
   */
  public static class WikipediaAppConfig extends Config {

    @Nullable
    public final String clusteringAlgorithm;

    public WikipediaAppConfig() {
      this(null);
    }

    public WikipediaAppConfig(@Nullable String clusteringAlgorithm) {
      this.clusteringAlgorithm = clusteringAlgorithm == null ? "lda" : clusteringAlgorithm;
    }
  }
}
