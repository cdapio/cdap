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
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.report.ReportGenerationApp;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.twill.filesystem.Location;

/**
 * spark main
 */
public class SparkPersistRunRecordMain implements JavaSparkMain {
  private TMSSubscriber tmsSubscriber;

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();
    Admin admin = sec.getAdmin();
    if (!admin.datasetExists(ReportGenerationApp.RUN_META_FILESET)) {
      admin.createDataset(ReportGenerationApp.RUN_META_FILESET, FileSet.class.getName(),
                          FileSetProperties.builder().build());
    }
    tmsSubscriber = new TMSSubscriber(sec.getMessagingContext().getMessageFetcher(),
                                      getDatasetBaseLocation(sec));
    tmsSubscriber.start();
    try {
      tmsSubscriber.join();
    } catch (InterruptedException ie) {
      tmsSubscriber.requestStop();
      tmsSubscriber.interrupt();
    }
  }


  private Location getDatasetBaseLocation(JavaSparkExecutionContext sec) {
    return Transactionals.execute(sec, context -> {
      FileSet fileSet = context.getDataset(ReportGenerationApp.RUN_META_FILESET);
      return fileSet.getBaseLocation();
    });
  }
}
