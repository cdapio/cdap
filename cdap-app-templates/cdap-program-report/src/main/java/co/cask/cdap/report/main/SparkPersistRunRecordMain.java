/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.main;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.report.ReportGenerationApp;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * spark main class, starts and waits for the tms subscriber thread to read run record meta and write to files
 */
public class SparkPersistRunRecordMain implements JavaSparkMain {
  private TMSSubscriber tmsSubscriber;
  private static final Logger LOG = LoggerFactory.getLogger(SparkPersistRunRecordMain.class);
  private static final SampledLogging SAMPLED_LOGGING = new SampledLogging(LOG, 100);

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();
    Admin admin = sec.getAdmin();
    if (!admin.datasetExists(ReportGenerationApp.RUN_META_FILESET)) {
      admin.createDataset(ReportGenerationApp.RUN_META_FILESET, FileSet.class.getName(),
                          FileSetProperties.builder().build());
    }

    tmsSubscriber = new TMSSubscriber(sec.getMessagingContext().getMessageFetcher(),
                                      getDatasetBaseLocationWithRetry(sec), sec.getRuntimeArguments());
    tmsSubscriber.start();
    try {
      tmsSubscriber.join();
    } catch (InterruptedException ie) {
      tmsSubscriber.requestStop();
      tmsSubscriber.interrupt();
    }
  }

  /**
   * Retry on dataset instantiation exception un-till we get the dataset and return location from the fileset.
   *
   * @return Location base location
   * @throws InterruptedException
   */
  private Location getDatasetBaseLocationWithRetry(JavaSparkExecutionContext sec) throws InterruptedException {
    while (true) {
      try {
        return Transactionals.execute(sec, context -> {
          FileSet fileSet = context.getDataset(ReportGenerationApp.RUN_META_FILESET);
          return fileSet.getBaseLocation();
        });
      } catch (RuntimeException e) {
        // retry on dataset exception
        if (e instanceof DatasetInstantiationException) {
          SAMPLED_LOGGING.logWarning(
            String.format("Exception while trying to get dataset %s", ReportGenerationApp.RUN_META_FILESET), e);
        } else {
          throw e;
        }
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
  }
}
