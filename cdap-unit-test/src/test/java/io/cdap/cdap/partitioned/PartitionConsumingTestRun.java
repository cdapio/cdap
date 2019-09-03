/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package io.cdap.cdap.partitioned;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.dataset.lib.PartitionDetail;
import io.cdap.cdap.api.dataset.lib.PartitionFilter;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.ProgramManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.base.TestFrameworkTestBase;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Test that MapReduce and Worker can incrementally process partitions.
 */
public class PartitionConsumingTestRun extends TestFrameworkTestBase {
  private static final String LINE1 = "a b a";
  private static final String LINE2 = "b a b";
  private static final String LINE3 = "c c c";

  @Test
  public void testMapReduceConsumer() throws Exception {
    testWordCountOnFileSet(new Function<ApplicationManager, ProgramManager>() {
      @Override
      public ProgramManager apply(ApplicationManager input) {
        return input.getMapReduceManager(AppWithPartitionConsumers.WordCountMapReduce.NAME).start();
      }
    }, true);
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "default",
                                               Constants.Metrics.Tag.APP, "AppWithPartitionConsumers",
                                               Constants.Metrics.Tag.MAPREDUCE, "WordCountMapReduce",
                                               Constants.Metrics.Tag.MR_TASK_TYPE, "r");
    long totalIn = getMetricsManager().getTotalMetric(tags, "system.process.entries.in");
    long totalOut = getMetricsManager().getTotalMetric(tags, "system.process.entries.out");
    Assert.assertEquals(9, totalIn);
    Assert.assertEquals(10, totalOut);
  }

  @Test
  public void testWorkerConsumer() throws Exception {
    testWordCountOnFileSet(new Function<ApplicationManager, ProgramManager>() {
      @Override
      public ProgramManager apply(ApplicationManager input) {
        return input.getWorkerManager(AppWithPartitionConsumers.WordCountWorker.NAME).start();
      }
    }, false);
  }

  private void testWordCountOnFileSet(Function<ApplicationManager, ProgramManager> runProgram,
                                      boolean produceOutputPartitionEachRun) throws Exception {
    ApplicationManager applicationManager = deployApplication(AppWithPartitionConsumers.class);

    ServiceManager serviceManager = applicationManager.getServiceManager("DatasetService").start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    URL serviceURL = serviceManager.getServiceURL();

    // write a file to the file set using the service and run the WordCount MapReduce job on that one partition
    createPartition(serviceURL, LINE1, "1");

    ProgramManager programManager = runProgram.apply(applicationManager);
    programManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    Assert.assertEquals(new Long(2), getCount(serviceURL, "a"));
    Assert.assertEquals(new Long(1), getCount(serviceURL, "b"));
    Assert.assertEquals(new Long(0), getCount(serviceURL, "c"));

    // create two additional partitions
    createPartition(serviceURL, LINE2, "2");
    createPartition(serviceURL, LINE3, "3");

    // running the program job now processes these two new partitions (LINE2 and LINE3) and updates the counts
    // dataset accordingly
    programManager = runProgram.apply(applicationManager);
    programManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 5, TimeUnit.MINUTES);

    Assert.assertEquals(new Long(3), getCount(serviceURL, "a"));
    Assert.assertEquals(new Long(3), getCount(serviceURL, "b"));
    Assert.assertEquals(new Long(3), getCount(serviceURL, "c"));

    // running the program without adding new partitions does not affect the counts dataset
    programManager = runProgram.apply(applicationManager);
    programManager.waitForRuns(ProgramRunStatus.COMPLETED, 3, 5, TimeUnit.MINUTES);

    Assert.assertEquals(new Long(3), getCount(serviceURL, "a"));
    Assert.assertEquals(new Long(3), getCount(serviceURL, "b"));
    Assert.assertEquals(new Long(3), getCount(serviceURL, "c"));

    DataSetManager<PartitionedFileSet> outputLines = getDataset("outputLines");
    Set<PartitionDetail> partitions = outputLines.get().getPartitions(PartitionFilter.ALWAYS_MATCH);

    // each of the three MapReduce runs produces an output partition (even if there's no input data)
    // however, Worker run doesn't produce a new output partition if there's no new input partition
    Assert.assertEquals(produceOutputPartitionEachRun ? 3 : 2, partitions.size());

    // we only store the counts to the "outputLines" dataset
    List<String> expectedCounts = Lists.newArrayList("1", "1", "2", "2", "3");
    List<String> outputRecords = getDataFromExplore("outputLines");
    Collections.sort(outputRecords);
    Assert.assertEquals(expectedCounts, outputRecords);
  }

  private List<String> getDataFromExplore(String dsName) throws Exception {
    try (Connection connection = getQueryClient()) {
      ResultSet results = connection.prepareStatement("SELECT * FROM dataset_" + dsName).executeQuery();

      List<String> cleanRecords = new ArrayList<>();
      while (results.next()) {
        cleanRecords.add(results.getString(1));
      }
      return cleanRecords;
    }
  }

  private void createPartition(URL serviceUrl, String body, String time) throws IOException {
    HttpResponse response =
      executeHttp(HttpRequest.put(new URL(serviceUrl, "lines?time=" + time)).withBody(body).build());
    Assert.assertEquals(200, response.getResponseCode());
  }

  private Long getCount(URL serviceUrl, String word) throws IOException {
    HttpResponse response =
      executeHttp(HttpRequest.get(new URL(serviceUrl, "counts?word=" + word)).build());
    Assert.assertEquals(200, response.getResponseCode());
    return Long.valueOf(response.getResponseBodyAsString());
  }
}
