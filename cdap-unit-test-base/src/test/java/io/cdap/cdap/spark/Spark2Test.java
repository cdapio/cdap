/*
 * Copyright © 2017-2021 Cask Data, Inc.
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

package io.cdap.cdap.spark;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.ObjectStore;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.DefaultId;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spark.app.CharCountProgram;
import io.cdap.cdap.spark.app.PythonSpark2;
import io.cdap.cdap.spark.app.ScalaCharCountProgram;
import io.cdap.cdap.spark.app.ScalaCrossNSProgram;
import io.cdap.cdap.spark.app.ScalaSparkServiceProgram;
import io.cdap.cdap.spark.app.Spark2TestApp;
import io.cdap.cdap.spark.app.SparkAppUsingLocalFiles;
import io.cdap.cdap.spark.app.SparkAppUsingObjectStore;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestBase;
import io.cdap.cdap.test.TestConfiguration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Spark2Test extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String TEST_STRING_1 = "persisted data";
  private static final String TEST_STRING_2 = "distributed systems";

  private static final Map<Class<? extends Application>, File> ARTIFACTS = new IdentityHashMap<>();

  @BeforeClass
  public static void init() throws IOException {
    ARTIFACTS.put(SparkAppUsingObjectStore.class, createArtifactJar(SparkAppUsingObjectStore.class));
    ARTIFACTS.put(SparkAppUsingLocalFiles.class, createArtifactJar(SparkAppUsingLocalFiles.class));
    ARTIFACTS.put(Spark2TestApp.class, createArtifactJar(Spark2TestApp.class));
  }

  @Test
  public void testSpark2Service() throws Exception {
    ApplicationManager applicationManager = deploy(NamespaceId.DEFAULT, Spark2TestApp.class);
    SparkManager manager = applicationManager.getSparkManager(ScalaSparkServiceProgram.class.getSimpleName()).start();

    URL url = manager.getServiceURL(5, TimeUnit.MINUTES);
    Assert.assertNotNull(url);

    // GET request to sum n numbers.
    URL sumURL = url.toURI().resolve("sum?n=" + Joiner.on("&n=").join(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).toURL();
    HttpURLConnection urlConn = (HttpURLConnection) sumURL.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, urlConn.getResponseCode());
    try (InputStream is = urlConn.getInputStream()) {
      Assert.assertEquals(55, Integer.parseInt(new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8)));
    }
  }

  @Test
  public void testSparkWithObjectStore() throws Exception {
    ApplicationManager applicationManager = deploy(NamespaceId.DEFAULT, SparkAppUsingObjectStore.class);

    DataSetManager<ObjectStore<String>> keysManager = getDataset("keys");
    prepareInputData(keysManager);

    SparkManager sparkManager = applicationManager.getSparkManager(CharCountProgram.class.getSimpleName()).start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    sparkManager.waitForStopped(60, TimeUnit.SECONDS);

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
    ApplicationManager applicationManager = deploy(NamespaceId.DEFAULT, SparkAppUsingObjectStore.class);

    DataSetManager<ObjectStore<String>> keysManager = getDataset("keys");
    prepareInputData(keysManager);

    SparkManager sparkManager = applicationManager.getSparkManager(ScalaCharCountProgram.class.getSimpleName()).start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    sparkManager.waitForStopped(60, TimeUnit.SECONDS);

    DataSetManager<KeyValueTable> countManager = getDataset("count");
    checkOutputData(countManager);
  }

  @Test
  public void testScalaSparkCrossNSStream() throws Exception {
    // create a namespace for input and create a file set instance
    NamespaceMeta inputNSMeta = new NamespaceMeta.Builder().setName("inputSpaceForSpark").build();
    getNamespaceAdmin().create(inputNSMeta);
    DatasetId inputDatasetId = inputNSMeta.getNamespaceId().dataset("input");
    addDatasetInstance(FileSet.class.getName(), inputDatasetId,
                       FileSetProperties.builder().setInputFormat(TextInputFormat.class).build());

    // create a namespace for dataset and add the dataset instance in it
    NamespaceMeta outputNSMeta = new NamespaceMeta.Builder().setName("crossNSDataset").build();
    getNamespaceAdmin().create(outputNSMeta);
    addDatasetInstance(outputNSMeta.getNamespaceId().dataset("count"), "keyValueTable");

    // write something to the input dataset
    Location inputFile = this.<FileSet>getDataset(inputDatasetId).get().getLocation("inputFile");
    try (PrintStream printer = new PrintStream(inputFile.getOutputStream(), true, "UTF-8")) {
      for (int i = 0; i < 50; i++) {
        printer.println(String.valueOf(i));
      }
    }

    // deploy the spark app in another namespace (default)
    ApplicationManager applicationManager = deploy(NamespaceId.DEFAULT, SparkAppUsingObjectStore.class);

    Map<String, String> args = new HashMap<>();
    args.put(ScalaCrossNSProgram.INPUT_NAMESPACE(), inputNSMeta.getNamespaceId().getNamespace());
    args.put(ScalaCrossNSProgram.OUTPUT_NAMESPACE(), outputNSMeta.getNamespaceId().getNamespace());
    args.put(ScalaCrossNSProgram.OUTPUT_NAME(), "count");

    FileSetArguments.setInputPath(args, "inputFile");

    SparkManager sparkManager =
      applicationManager.getSparkManager(ScalaCrossNSProgram.class.getSimpleName()).start(args);
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    sparkManager.waitForStopped(60, TimeUnit.SECONDS);

    // get the dataset from the other namespace where we expect it to exist and compare the data
    DataSetManager<KeyValueTable> countManager = getDataset(outputNSMeta.getNamespaceId().dataset("count"));
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

    ApplicationManager applicationManager = deploy(NamespaceId.DEFAULT, SparkAppUsingObjectStore.class);
    SparkManager sparkManager =
      applicationManager.getSparkManager(ScalaCharCountProgram.class.getSimpleName()).start(args);
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    sparkManager.waitForStopped(60, TimeUnit.SECONDS);

    DataSetManager<KeyValueTable> countManager = getDataset("count");
    checkOutputData(countManager);
  }

  @Test
  public void testSparkWithLocalFiles() throws Exception {
    testSparkWithLocalFiles(SparkAppUsingLocalFiles.class,
                            SparkAppUsingLocalFiles.JavaSparkUsingLocalFiles.class.getSimpleName(), "java");
    testSparkWithLocalFiles(SparkAppUsingLocalFiles.class,
                            SparkAppUsingLocalFiles.ScalaSparkUsingLocalFiles.class.getSimpleName(), "scala");
  }

  @Test
  public void testPySpark() throws Exception {
    ApplicationManager appManager = deploy(NamespaceId.DEFAULT, Spark2TestApp.class);

    // Write some data to a local file
    File inputFile = TEMP_FOLDER.newFile();
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile.toPath(), StandardCharsets.UTF_8)) {
      for (int i = 0; i < 100; i++) {
        writer.write("Event " + i);
        writer.newLine();
      }
    }

    File outputDir = new File(TMP_FOLDER.newFolder(), "output");
    appManager.getSparkManager(PythonSpark2.class.getSimpleName())
      .startAndWaitForRun(ImmutableMap.of("input.file", inputFile.getAbsolutePath(),
                                          "output.path", outputDir.getAbsolutePath()),
                          ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    // Verify the result
    File resultFile = DirUtils.listFiles(outputDir).stream()
      .filter(f -> !f.getName().endsWith(".crc"))
      .filter(f -> !f.getName().startsWith("_SUCCESS"))
      .findFirst()
      .orElse(null);
    Assert.assertNotNull(resultFile);

    List<String> lines = Files.readAllLines(resultFile.toPath(), StandardCharsets.UTF_8);
    Assert.assertFalse(lines.isEmpty());

    // Expected only even number
    int count = 0;
    for (String line : lines) {
      line = line.trim();
      if (!line.isEmpty()) {
        Assert.assertEquals("Event " + count, line);
        count += 2;
      }
    }

    Assert.assertEquals(100, count);

    final Map<String, String> tags = ImmutableMap.of(
      Constants.Metrics.Tag.NAMESPACE, NamespaceId.DEFAULT.getNamespace(),
      Constants.Metrics.Tag.APP, Spark2TestApp.class.getSimpleName(),
      Constants.Metrics.Tag.SPARK, PythonSpark2.class.getSimpleName());

    Tasks.waitFor(100L, () ->  getMetricsManager().getTotalMetric(tags, "user.body"),
                  5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
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


  /**
   * Creates an artifact jar by tracing dependency from the given {@link Application} class.
   */
  private static File createArtifactJar(Class<? extends Application> appClass) throws IOException {
    return new File(AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                     appClass).toURI());
  }

  private ApplicationManager deploy(NamespaceId namespaceId, Class<? extends Application> appClass) throws Exception {
    ArtifactId artifactId = new ArtifactId(namespaceId.getNamespace(),
                                           appClass.getSimpleName(), "1.0-SNAPSHOT");
    addArtifact(artifactId, ARTIFACTS.get(appClass));
    AppRequest<?> appRequest = new AppRequest<>(new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
                                                null);
    return deployApplication(namespaceId.app(appClass.getSimpleName()), appRequest);
  }

  private void testSparkWithLocalFiles(Class<? extends Application> appClass,
                                       String sparkProgram, String prefix) throws Exception {
    ApplicationManager applicationManager = deploy(NamespaceId.DEFAULT, appClass);
    URI localFile = createLocalPropertiesFile(prefix);

    SparkManager sparkManager = applicationManager.getSparkManager(sparkProgram)
      .start(Collections.singletonMap(SparkAppUsingLocalFiles.LOCAL_FILE_RUNTIME_ARG, localFile.toString()));
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    sparkManager.waitForStopped(120, TimeUnit.SECONDS);

    DataSetManager<KeyValueTable> kvTableManager = getDataset(SparkAppUsingLocalFiles.OUTPUT_DATASET_NAME);
    KeyValueTable kvTable = kvTableManager.get();
    Map<String, String> expected = ImmutableMap.of("a", "1", "b", "2", "c", "3");
    List<byte[]> deleteKeys = new ArrayList<>();
    try (CloseableIterator<KeyValue<byte[], byte[]>> scan = kvTable.scan(null, null)) {
      for (int i = 0; i < 3; i++) {
        KeyValue<byte[], byte[]> next = scan.next();
        Assert.assertEquals(expected.get(Bytes.toString(next.getKey())), Bytes.toString(next.getValue()));
        deleteKeys.add(next.getKey());
      }
      Assert.assertFalse(scan.hasNext());
    }

    // Cleanup after run
    kvTableManager.flush();
    for (byte[] key : deleteKeys) {
      kvTable.delete(key);
    }
    kvTableManager.flush();
  }

  private URI createLocalPropertiesFile(String filePrefix) throws IOException {
    File file = TMP_FOLDER.newFile(filePrefix + "-local.properties");
    try (OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(file))) {
      out.write("a=1\n");
      out.write("b = 2\n");
      out.write("c= 3");
    }
    return file.toURI();
  }
}
