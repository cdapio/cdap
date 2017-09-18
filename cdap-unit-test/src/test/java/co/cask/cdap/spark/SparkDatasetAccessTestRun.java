/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.spark;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.DefaultId;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spark.app.CharCountProgram;
import co.cask.cdap.spark.app.ScalaCharCountProgram;
import co.cask.cdap.spark.app.ScalaCrossNSProgram;
import co.cask.cdap.spark.app.ScalaSparkLogParser;
import co.cask.cdap.spark.app.SparkAppUsingGetDataset;
import co.cask.cdap.spark.app.SparkAppUsingObjectStore;
import co.cask.cdap.spark.app.SparkLogParser;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Unit-tests for testing Spark program.
 */
public class SparkDatasetAccessTestRun extends TestFrameworkTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  private static final String TEST_STRING_1 = "persisted data";
  private static final String TEST_STRING_2 = "distributed systems";

  private static final Map<Class<? extends Application>, File> ARTIFACTS = new IdentityHashMap<>();

  @BeforeClass
  public static void init() throws IOException {
    ARTIFACTS.put(SparkAppUsingObjectStore.class, createArtifactJar(SparkAppUsingObjectStore.class));
    ARTIFACTS.put(SparkAppUsingGetDataset.class, createArtifactJar(SparkAppUsingGetDataset.class));
  }

  private ApplicationManager deploy(Class<? extends Application> appClass) throws Exception {
    return deployWithArtifact(appClass, ARTIFACTS.get(appClass));
  }

  private ApplicationManager deploy(NamespaceId namespaceId, Class<? extends Application> appClass) throws Exception {
    return deployWithArtifact(namespaceId, appClass, ARTIFACTS.get(appClass));
  }

  @Test
  public void testSparkWithObjectStore() throws Exception {
    ApplicationManager applicationManager = deploy(SparkAppUsingObjectStore.class);

    DataSetManager<ObjectStore<String>> keysManager = getDataset("keys");
    prepareInputData(keysManager);

    SparkManager sparkManager = applicationManager.getSparkManager(CharCountProgram.class.getSimpleName()).start();
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> countManager = getDataset("count");
    checkOutputData(countManager);

    // validate that the table emitted metrics
    // one read + one write in beforeSubmit(), increment (= read + write) in main -> 4
    Tasks.waitFor(4L, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        Collection<MetricTimeSeries> metrics =
          getMetricsManager().query(new MetricDataQuery(
            0,
            System.currentTimeMillis() / 1000L,
            Integer.MAX_VALUE,
            "system." + Constants.Metrics.Name.Dataset.OP_COUNT,
            AggregationFunction.SUM,
            ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, DefaultId.NAMESPACE.getNamespace(),
                            Constants.Metrics.Tag.APP, SparkAppUsingObjectStore.class.getSimpleName(),
                            Constants.Metrics.Tag.SPARK, CharCountProgram.class.getSimpleName(),
                            Constants.Metrics.Tag.DATASET, "totals"),
            Collections.<String>emptyList()));
        if (metrics.isEmpty()) {
          return 0L;
        }
        Assert.assertEquals(1, metrics.size());
        MetricTimeSeries ts = metrics.iterator().next();
        Assert.assertEquals(1, ts.getTimeValues().size());
        return ts.getTimeValues().get(0).getValue();
      }
    }, 10L, TimeUnit.SECONDS, 50L, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testScalaSparkWithObjectStore() throws Exception {
    ApplicationManager applicationManager = deploy(SparkAppUsingObjectStore.class);

    DataSetManager<ObjectStore<String>> keysManager = getDataset("keys");
    prepareInputData(keysManager);

    SparkManager sparkManager = applicationManager.getSparkManager(ScalaCharCountProgram.class.getSimpleName()).start();
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> countManager = getDataset("count");
    checkOutputData(countManager);
  }

  @Test
  public void testScalaSparkCrossNSStream() throws Exception {
    // create a namespace for stream and create the stream in it
    NamespaceMeta crossNSStreamMeta = new NamespaceMeta.Builder().setName("streamSpaceForSpark").build();
    getNamespaceAdmin().create(crossNSStreamMeta);
    StreamManager streamManager = getStreamManager(crossNSStreamMeta.getNamespaceId().stream("testStream"));

    // create a namespace for dataset and add the dataset instance in it
    NamespaceMeta crossNSDatasetMeta = new NamespaceMeta.Builder().setName("crossNSDataset").build();
    getNamespaceAdmin().create(crossNSDatasetMeta);
    addDatasetInstance(crossNSDatasetMeta.getNamespaceId().dataset("count"), "keyValueTable");

    // write something to the stream
    streamManager.createStream();
    for (int i = 0; i < 50; i++) {
      streamManager.send(String.valueOf(i));
    }

    // deploy the spark app in another namespace (default)
    ApplicationManager applicationManager = deploy(SparkAppUsingObjectStore.class);

    Map<String, String> args = ImmutableMap.of(ScalaCrossNSProgram.STREAM_NAMESPACE(),
                                               crossNSStreamMeta.getNamespaceId().getNamespace(),
                                               ScalaCrossNSProgram.DATASET_NAMESPACE(),
                                               crossNSDatasetMeta.getNamespaceId().getNamespace(),
                                               ScalaCrossNSProgram.DATASET_NAME(), "count");
    SparkManager sparkManager =
      applicationManager.getSparkManager(ScalaCrossNSProgram.class.getSimpleName()).start(args);
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    // get the dataset from the other namespace where we expect it to exist and compare the data
    DataSetManager<KeyValueTable> countManager = getDataset(crossNSDatasetMeta.getNamespaceId().dataset("count"));
    KeyValueTable results = countManager.get();
    for (int i = 0; i < 50; i++) {
      byte[] key = String.valueOf(i).getBytes(Charsets.UTF_8);
      Assert.assertArrayEquals(key, results.read(key));
    }
  }

  @Test
  public void testScalaSparkCrossNSDataset() throws Exception {
    // Deploy and create a dataset in namespace datasetSpaceForSpark
    NamespaceMeta inputDSNSMeta = new NamespaceMeta.Builder().setName("datasetSpaceForSpark").build();
    getNamespaceAdmin().create(inputDSNSMeta);
    deploy(inputDSNSMeta.getNamespaceId(), SparkAppUsingObjectStore.class);
    DataSetManager<ObjectStore<String>> keysManager = getDataset(inputDSNSMeta.getNamespaceId().dataset("keys"));
    prepareInputData(keysManager);

    Map<String, String> args = ImmutableMap.of(ScalaCharCountProgram.INPUT_DATASET_NAMESPACE(),
                                               inputDSNSMeta.getNamespaceId().getNamespace(),
                                               ScalaCharCountProgram.INPUT_DATASET_NAME(), "keys");

    ApplicationManager applicationManager = deploy(SparkAppUsingObjectStore.class);
    SparkManager sparkManager =
      applicationManager.getSparkManager(ScalaCharCountProgram.class.getSimpleName()).start(args);
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> countManager = getDataset("count");
    checkOutputData(countManager);
  }

  @Test
  public void testSparkWithGetDataset() throws Exception {
    testSparkWithGetDataset(SparkAppUsingGetDataset.class, SparkLogParser.class.getSimpleName());
    testSparkWithGetDataset(SparkAppUsingGetDataset.class, ScalaSparkLogParser.class.getSimpleName());
  }

  private void prepareInputData(DataSetManager<ObjectStore<String>> manager) {
    ObjectStore<String> keys = manager.get();
    keys.write(Bytes.toBytes(TEST_STRING_1), TEST_STRING_1);
    keys.write(Bytes.toBytes(TEST_STRING_2), TEST_STRING_2);
    manager.flush();
  }

  private void checkOutputData(DataSetManager<KeyValueTable> manager) {
    KeyValueTable count = manager.get();
    //read output and verify result
    byte[] val = count.read(Bytes.toBytes(TEST_STRING_1));
    Assert.assertTrue(val != null);
    Assert.assertEquals(Bytes.toInt(val), TEST_STRING_1.length());

    val = count.read(Bytes.toBytes(TEST_STRING_2));
    Assert.assertTrue(val != null);
    Assert.assertEquals(Bytes.toInt(val), TEST_STRING_2.length());
  }

  private void testSparkWithGetDataset(Class<? extends Application> appClass, String sparkProgram) throws Exception {
    ApplicationManager applicationManager = deploy(appClass);

    DataSetManager<FileSet> filesetManager = getDataset("logs");
    FileSet fileset = filesetManager.get();
    Location location = fileset.getLocation("nn");
    prepareInputFileSetWithLogData(location);

    Map<String, String> inputArgs = new HashMap<>();
    FileSetArguments.setInputPath(inputArgs, "nn");
    Map<String, String> args = new HashMap<>();
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "logs", inputArgs));
    args.put("input", "logs");
    args.put("output", "logStats");

    SparkManager sparkManager = applicationManager.getSparkManager(sparkProgram).start(args);
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> logStatsManager = getDataset("logStats");
    KeyValueTable logStatsTable = logStatsManager.get();
    validateGetDatasetOutput(logStatsTable);

    // Cleanup after run
    location.delete(true);
    logStatsManager.flush();
    try (CloseableIterator<KeyValue<byte[], byte[]>> scan = logStatsTable.scan(null, null)) {
      while (scan.hasNext()) {
        logStatsTable.delete(scan.next().getKey());
      }
    }
    logStatsManager.flush();
  }

  private void prepareInputFileSetWithLogData(Location location) throws IOException {
    try (OutputStreamWriter out = new OutputStreamWriter(location.getOutputStream())) {
      out.write("10.10.10.10 - FRED [18/Jan/2013:17:56:07 +1100] \"GET http://bar.com/image.jpg " +
                  "HTTP/1.1\" 200 50 \"http://foo.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                  "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                  ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                  "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 " +
                  "\"\" 62.24.11.25 images.com 1358492167 - Whatup\n");
      out.write("20.20.20.20 - BRAD [18/Jan/2013:17:56:07 +1100] \"GET http://bar.com/image.jpg " +
                  "HTTP/1.1\" 200 50 \"http://foo.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                  "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                  ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                  "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 " +
                  "\"\" 62.24.11.25 images.com 1358492167 - Whatup\n");
      out.write("10.10.10.10 - FRED [18/Jan/2013:17:56:07 +1100] \"GET http://bar.com/image.jpg " +
                  "HTTP/1.1\" 404 50 \"http://foo.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                  "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                  ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                  "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 " +
                  "\"\" 62.24.11.25 images.com 1358492167 - Whatup\n");
      out.write("10.10.10.10 - FRED [18/Jan/2013:17:56:07 +1100] \"GET http://bar.com/image.jpg " +
                  "HTTP/1.1\" 200 50 \"http://foo.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                  "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                  ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                  "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 " +
                  "\"\" 62.24.11.25 images.com 1358492167 - Whatup\n");
      out.write("20.20.20.20 - BRAD [18/Jan/2013:17:56:07 +1100] \"GET http://bar.com/image.jpg " +
                  "HTTP/1.1\" 404 50 \"http://foo.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                  "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                  ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                  "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 " +
                  "\"\" 62.24.11.25 images.com 1358492167 - Whatup\n");
    }
  }

  private void validateGetDatasetOutput(KeyValueTable logStatsTable) {
    SparkAppUsingGetDataset.LogKey fredKey1 = new SparkAppUsingGetDataset.LogKey(
      "10.10.10.10", "FRED", "GET http://bar.com/image.jpg HTTP/1.1", 200);
    SparkAppUsingGetDataset.LogKey fredKey2 = new SparkAppUsingGetDataset.LogKey(
      "10.10.10.10", "FRED", "GET http://bar.com/image.jpg HTTP/1.1", 404);
    SparkAppUsingGetDataset.LogKey bradKey1 = new SparkAppUsingGetDataset.LogKey(
      "20.20.20.20", "BRAD", "GET http://bar.com/image.jpg HTTP/1.1", 200);
    SparkAppUsingGetDataset.LogKey bradKey2 = new SparkAppUsingGetDataset.LogKey(
      "20.20.20.20", "BRAD", "GET http://bar.com/image.jpg HTTP/1.1", 404);
    SparkAppUsingGetDataset.LogStats fredStats1 = new SparkAppUsingGetDataset.LogStats(2, 100);
    SparkAppUsingGetDataset.LogStats fredStats2 = new SparkAppUsingGetDataset.LogStats(1, 50);
    SparkAppUsingGetDataset.LogStats bradStats1 = new SparkAppUsingGetDataset.LogStats(1, 50);
    SparkAppUsingGetDataset.LogStats bradStats2 = new SparkAppUsingGetDataset.LogStats(1, 50);

    Map<SparkAppUsingGetDataset.LogKey, SparkAppUsingGetDataset.LogStats> expected = ImmutableMap.of(
      fredKey1, fredStats1, fredKey2, fredStats2, bradKey1, bradStats1, bradKey2, bradStats2
    );

    try (CloseableIterator<KeyValue<byte[], byte[]>> scan = logStatsTable.scan(null, null)) {
      // must have 4 records
      for (int i = 0; i < 4; i++) {
        Assert.assertTrue("Expected next for i = " + i, scan.hasNext());
        KeyValue<byte[], byte[]> next = scan.next();
        SparkAppUsingGetDataset.LogKey logKey =
          new Gson().fromJson(Bytes.toString(next.getKey()), SparkAppUsingGetDataset.LogKey.class);
        SparkAppUsingGetDataset.LogStats logStats =
          new Gson().fromJson(Bytes.toString(next.getValue()), SparkAppUsingGetDataset.LogStats.class);
        Assert.assertEquals(expected.get(logKey), logStats);
      }
      // no more records
      Assert.assertFalse(scan.hasNext());
    }
  }
}
