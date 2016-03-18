/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.partitioned;

import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.ProgramManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
    });
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
    });
  }

  private void testWordCountOnFileSet(Function<ApplicationManager, ProgramManager> runProgram) throws Exception {
    ApplicationManager applicationManager = deployApplication(AppWithPartitionConsumers.class);

    ServiceManager serviceManager = applicationManager.getServiceManager("DatasetService").start();
    serviceManager.waitForStatus(true);
    URL serviceURL = serviceManager.getServiceURL();

    // write a file to the file set using the service and run the WordCount MapReduce job on that one partition
    createPartition(serviceURL, LINE1, "1");

    ProgramManager programManager = runProgram.apply(applicationManager);
    programManager.waitForFinish(5, TimeUnit.MINUTES);

    Assert.assertEquals(new Long(2), getCount(serviceURL, "a"));
    Assert.assertEquals(new Long(1), getCount(serviceURL, "b"));
    Assert.assertEquals(new Long(0), getCount(serviceURL, "c"));

    // create two additional partitions
    createPartition(serviceURL, LINE2, "2");
    createPartition(serviceURL, LINE3, "3");

    // running the program job now processes these two new partitions (LINE2 and LINE3) and updates the counts
    // dataset accordingly
    programManager = runProgram.apply(applicationManager);
    programManager.waitForFinish(5, TimeUnit.MINUTES);

    Assert.assertEquals(new Long(3), getCount(serviceURL, "a"));
    Assert.assertEquals(new Long(3), getCount(serviceURL, "b"));
    Assert.assertEquals(new Long(3), getCount(serviceURL, "c"));

    // running the program without adding new partitions does not affect the counts dataset
    programManager = runProgram.apply(applicationManager);
    programManager.waitForFinish(5, TimeUnit.MINUTES);

    Assert.assertEquals(new Long(3), getCount(serviceURL, "a"));
    Assert.assertEquals(new Long(3), getCount(serviceURL, "b"));
    Assert.assertEquals(new Long(3), getCount(serviceURL, "c"));

    DataSetManager<PartitionedFileSet> outputLines = getDataset("outputLines");
    Set<PartitionDetail> partitions = outputLines.get().getPartitions(PartitionFilter.ALWAYS_MATCH);

    Assert.assertEquals(2, partitions.size());

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
      HttpRequests.execute(HttpRequest.put(new URL(serviceUrl, "lines?time=" + time)).withBody(body).build());
    Assert.assertEquals(200, response.getResponseCode());
  }

  private Long getCount(URL serviceUrl, String word) throws IOException {
    HttpResponse response =
      HttpRequests.execute(HttpRequest.get(new URL(serviceUrl, "counts?word=" + word)).build());
    Assert.assertEquals(200, response.getResponseCode());
    return Long.valueOf(response.getResponseBodyAsString());
  }
}
