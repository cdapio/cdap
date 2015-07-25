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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Test for {@link WikipediaPipelineApp}
 */
public class WikipediaPipelineAppTest extends TestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(WikipediaPipelineApp.class);
    // Setup input streams with test data
    createTestData();

    WorkflowManager workflowManager = applicationManager.getWorkflowManager(WikipediaPipelineWorkflow.NAME);
    // Test with default threshold. Workflow should not proceed beyond first condition.
    testWorkflowWithDefaultThreshold(workflowManager);

    // Test with a reduced threshold, so the workflow proceeds beyond the first predicate
    testWorkflowWithReducedThreshold(workflowManager);
  }

  private void createTestData() throws Exception {
    StreamManager likesStreamManager = getStreamManager(WikipediaPipelineApp.PAGE_TITLES_STREAM);
    likesStreamManager.send("{\n" +
                              "      \"name\": \"Metallica\"," +
                              "      \"id\": \"103636093053996\"," +
                              "      \"created_time\": \"2012-02-11T22:41:49+0000\"" +
                              "    }");
    likesStreamManager.send("{\n" +
                              "      \"name\": \"grunge\"," +
                              "      \"id\": \"58417452552\"," +
                              "      \"created_time\": \"2012-02-07T21:29:53+0000\"" +
                              "    }");
    StreamManager rawWikipediaStreamManager = getStreamManager(WikipediaPipelineApp.RAW_WIKIPEDIA_STREAM);
    URL wikipediaUrl = Thread.currentThread().getContextClassLoader().getResource("raw_wikipedia_data.txt");
    Assert.assertNotNull(wikipediaUrl);
    File wikipediaData = new File(wikipediaUrl.toURI());
    rawWikipediaStreamManager.send(wikipediaData, "text/plain");
  }

  private void testWorkflowWithDefaultThreshold(WorkflowManager workflowManager) throws Exception {
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
    DataSetManager<KeyValueTable> pageTitles = getDataset(WikipediaPipelineApp.PAGE_TITLES_DATASET);
    assertPageTitleDatasetContents(pageTitles.get());
    String pid = getLatestPid(workflowManager.getHistory());
    WorkflowTokenNodeDetail tokenAtCondition =
      workflowManager.getTokenAtNode(pid, WikipediaPipelineWorkflow.EnoughDataToProceed.class.getSimpleName(),
                                     WorkflowToken.Scope.USER, "result");
    boolean conditionResult = Boolean.parseBoolean(tokenAtCondition.getTokenDataAtNode().get("result"));
    Assert.assertFalse(conditionResult);
    assertAllDatasetContents(false);
  }

  private void testWorkflowWithReducedThreshold(WorkflowManager workflowManager) throws Exception {
    workflowManager.start(ImmutableMap.of("min.page.threshold", String.valueOf(1)));
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
    DataSetManager<KeyValueTable> pageTitles = getDataset(WikipediaPipelineApp.PAGE_TITLES_DATASET);
    assertPageTitleDatasetContents(pageTitles.get());
    String pid = getLatestPid(workflowManager.getHistory());
    WorkflowTokenNodeDetail tokenAtCondition =
      workflowManager.getTokenAtNode(pid, WikipediaPipelineWorkflow.EnoughDataToProceed.class.getSimpleName(),
                                     WorkflowToken.Scope.USER, "result");
    boolean conditionResult = Boolean.parseBoolean(tokenAtCondition.getTokenDataAtNode().get("result"));
    Assert.assertTrue(conditionResult);
    assertAllDatasetContents(true);
  }

  @Nullable
  private String getLatestPid(List<RunRecord> history) {
    String pid = null;
    long latestStartTime = 0;
    for (RunRecord runRecord : history) {
      // ok to use start ts, since we ensure that the next run begins after the previous run finishes in the test
      if (runRecord.getStartTs() > latestStartTime) {
        latestStartTime = runRecord.getStartTs();
        pid = runRecord.getPid();
      }
    }
    return pid;
  }

  private void assertAllDatasetContents(boolean continueConditionSucceeded) throws Exception {
    // verify contents of raw wikipedia dataset
    DataSetManager<KeyValueTable> rawWikiData = getDataset(WikipediaPipelineApp.RAW_WIKIPEDIA_DATASET);
//    assertRawWikipediaData(rawWikiData.get(), continueConditionSucceeded);
    // verify contents of normalized wikipedia dataset
    DataSetManager<KeyValueTable> filteredWikiData = getDataset(WikipediaPipelineApp.NORMALIZED_WIKIPEDIA_DATASET);
//    assertFilteredDatasetContents(filteredWikiData.get(), continueConditionSucceeded);
    // verify contents of spark LDA dataset
    DataSetManager<Table> sparkLDA = getDataset(WikipediaPipelineApp.SPARK_LDA_OUTPUT_DATASET);
    assertSparkLDAOutput(sparkLDA.get(), continueConditionSucceeded);
    // verify contents of MapReduce top N dataset
    DataSetManager<KeyValueTable> mapreduceTopN = getDataset(WikipediaPipelineApp.MAPREDUCE_TOPN_OUTPUT);
    assertMapReduceTopNOutput(mapreduceTopN.get(), continueConditionSucceeded);
  }

  private void assertPageTitleDatasetContents(KeyValueTable pageTitleDataset) {
    CloseableIterator<KeyValue<byte[], byte[]>> scanner = pageTitleDataset.scan(null, null);
    KeyValue<byte[], byte[]> next = scanner.next();
    Assert.assertNotNull(next);
    String key1 = Bytes.toString(next.getKey());
    String value1 = Bytes.toString(next.getValue());
    next = scanner.next();
    Assert.assertNotNull(next);
    String key2 = Bytes.toString(next.getKey());
    String value2 = Bytes.toString(next.getValue());
    if ("103636093053996".equals(key1)) {
      Assert.assertEquals("Metallica", value1);
      Assert.assertEquals("58417452552", key2);
      Assert.assertEquals("grunge", value2);
    } else {
      Assert.assertEquals("58417452552", key1);
      Assert.assertEquals("grunge", value1);
      Assert.assertEquals("103636093053996", key2);
      Assert.assertEquals("Metallica", value2);
    }
  }

  private void assertRawWikipediaData(KeyValueTable rawWikiDataDataset, boolean continueConditionSucceeded) {
    CloseableIterator<KeyValue<byte[], byte[]>> scanner = rawWikiDataDataset.scan(null, null);
    if (!continueConditionSucceeded) {
      Assert.assertFalse(scanner.hasNext());
      return;
    }
    // Should have 2 records
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(scanner.hasNext());
      Assert.assertNotNull(scanner.next());
    }
    Assert.assertFalse(scanner.hasNext());
  }

  private void assertFilteredDatasetContents(KeyValueTable filteredDataset, boolean continueConditionSucceeded) {
    CloseableIterator<KeyValue<byte[], byte[]>> scanner = filteredDataset.scan(null, null);
    if (!continueConditionSucceeded) {
      Assert.assertFalse(scanner.hasNext());
      return;
    }
    // should have 2 records
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(scanner.hasNext());
      Assert.assertNotNull(scanner.next());
    }
    Assert.assertFalse(scanner.hasNext());
  }

  private void assertSparkLDAOutput(Table ldaOutput, boolean continueConditionSucceeded) {
    Scanner scanner = ldaOutput.scan(null, null);
    if (!continueConditionSucceeded) {
      Assert.assertNull(scanner.next());
      return;
    }
    for (int i = 0; i < 10; i++) {
      Row next = scanner.next();
      Assert.assertNotNull(next);
      Assert.assertEquals(i, Bytes.toInt(next.getRow()));
      Assert.assertEquals(10, next.getColumns().size());
    }
    Assert.assertNull(scanner.next());
  }

  private void assertMapReduceTopNOutput(KeyValueTable keyValueTable, boolean continueConditionSucceeded) {
    CloseableIterator<KeyValue<byte[], byte[]>> scanner = keyValueTable.scan(null, null);
    if (!continueConditionSucceeded) {
      Assert.assertFalse(scanner.hasNext());
      return;
    }
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(scanner.hasNext());
      Assert.assertNotNull(scanner.next());
    }
    Assert.assertFalse(scanner.hasNext());
  }
}
