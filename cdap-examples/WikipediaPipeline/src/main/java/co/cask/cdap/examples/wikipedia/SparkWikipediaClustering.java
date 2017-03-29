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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.spark.AbstractSpark;
import org.apache.spark.SparkConf;

/**
 * Spark program that executes in a workflow and analyzes wikipedia data
 */
public class SparkWikipediaClustering extends AbstractSpark {
  public static final String NAME = SparkWikipediaClustering.class.getSimpleName();

  private final WikipediaPipelineApp.WikipediaAppConfig appConfig;

  public SparkWikipediaClustering(WikipediaPipelineApp.WikipediaAppConfig appConfig) {
    this.appConfig = appConfig;
  }

  @Override
  protected void initialize() throws Exception {
    SparkConf sparkConf = new SparkConf()
      .set("spark.driver.extraJavaOptions",
           "-verbose:gc -Xloggc:<LOG_DIR>/gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps " +
             "-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M " +
             "-XX:MaxPermSize=128m")
      .set("spark.executor.extraJavaOptions",
           "-verbose:gc -Xloggc:<LOG_DIR>/gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps " +
             "-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M " +
             "-XX:MaxPermSize=128m");
    getContext().setSparkConf(sparkConf);
  }

  @Override
  protected void configure() {
    if ("lda".equals(appConfig.clusteringAlgorithm)) {
      setDescription("A Spark program that analyzes wikipedia data using Latent Dirichlet Allocation (LDA).");
      setMainClass(ScalaSparkLDA.class);
    } else if ("kmeans".equals(appConfig.clusteringAlgorithm)) {
      setDescription("A Spark program that analyzes wikipedia data using K-Means.");
      setMainClass(ScalaSparkKMeans.class);
    } else {
      throw new IllegalArgumentException("Only 'lda' and 'kmeans' are supported as clustering algorithms. " +
                                           "Found " + appConfig.clusteringAlgorithm);
    }

    setName(NAME + "-" + appConfig.clusteringAlgorithm.toUpperCase());
    setDriverResources(new Resources(1024));
    setExecutorResources(new Resources(1024));
  }
}
