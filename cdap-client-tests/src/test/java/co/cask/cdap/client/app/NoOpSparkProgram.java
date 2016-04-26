/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.client.app;

import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class NoOpSparkProgram implements JavaSparkMain {

  private static final Logger LOG = LoggerFactory.getLogger(NoOpSparkProgram.class);

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();
    JavaPairRDD<Long, String> streamRDD = sec.fromStream(AllProgramsApp.STREAM_NAME, String.class);
    LOG.info("Stream events: {}", streamRDD.count());

    JavaPairRDD<byte[], byte[]> datasetRDD = sec.fromDataset(AllProgramsApp.DATASET_NAME);
    LOG.info("Dataset pairs: {}", datasetRDD.count());

    sec.saveAsDataset(datasetRDD, AllProgramsApp.DATASET_NAME2);

    datasetRDD = sec.fromDataset(AllProgramsApp.DATASET_NAME3);
    LOG.info("Dataset pairs: {}", datasetRDD.count());

    sec.saveAsDataset(datasetRDD, AllProgramsApp.DATASET_NAME3);
  }
}
