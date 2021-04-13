/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.common.Scope;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.ObjectMappedTable;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.spark.app.ClassicSparkProgram;
import io.cdap.cdap.spark.app.DatasetSQLSpark;
import io.cdap.cdap.spark.app.Person;
import io.cdap.cdap.spark.app.PythonSpark;
import io.cdap.cdap.spark.app.ScalaClassicSparkProgram;
import io.cdap.cdap.spark.app.ScalaDynamicSpark;
import io.cdap.cdap.spark.app.ScalaSparkLogParser;
import io.cdap.cdap.spark.app.SparkAppUsingGetDataset;
import io.cdap.cdap.spark.app.SparkLogParser;
import io.cdap.cdap.spark.app.SparkServiceProgram;
import io.cdap.cdap.spark.app.TestSparkApp;
import io.cdap.cdap.spark.app.TransactionSpark;
import io.cdap.cdap.spark.app.plugin.PluggableFunc;
import io.cdap.cdap.spark.app.plugin.StringLengthFunc;
import io.cdap.cdap.spark.app.plugin.StringLengthUDT;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.cdap.test.base.TestFrameworkTestBase;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Unit-tests for testing Spark program.
 */
public class SparkTest extends TestFrameworkTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(SparkTest.class);

  private static final Map<Class<? extends Application>, File> ARTIFACTS = new IdentityHashMap<>();

  @BeforeClass
  public static void init() throws IOException {
    ARTIFACTS.put(TestSparkApp.class, createArtifactJar(TestSparkApp.class));
    ARTIFACTS.put(SparkAppUsingGetDataset.class, createArtifactJar(SparkAppUsingGetDataset.class));
  }

  private ApplicationManager deploy(Class<? extends Application> appClass) throws Exception {
    return deployWithArtifact(appClass, ARTIFACTS.get(appClass));
  }

  @Test @Ignore("For this to work in spark 2 and later DefaultSource should implement" +
    "org.apache.spark.sql.execution.datasources.FileFormat")
  public void testDatasetSQL() throws Exception {
    ApplicationManager appManager = deploy(TestSparkApp.class);

    DataSetManager<ObjectMappedTable<Person>> tableManager = getDataset("PersonTable");

    ObjectMappedTable<Person> table = tableManager.get();
    table.write("1", new Person("Bob", 10));
    table.write("2", new Person("Bill", 20));
    table.write("3", new Person("Berry", 30));
    tableManager.flush();

    SparkManager sparkManager = appManager.getSparkManager(DatasetSQLSpark.class.getSimpleName());
    sparkManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    // The program executes "SELECT * FROM Person WHERE age > 10", hence expected two new entries for Bill and Berry.
    tableManager.flush();

    Person person = table.read("new:2");
    Assert.assertEquals("Bill", person.name());
    Assert.assertEquals(20, person.age());

    person = table.read("new:3");
    Assert.assertEquals("Berry", person.name());
    Assert.assertEquals(30, person.age());

    // Shouldn't have new Bob
    Assert.assertNull(table.read("new:1"));
  }

  @Test
  public void testClassicSpark() throws Exception {
    ApplicationManager appManager = deploy(TestSparkApp.class);

    for (Class<?> sparkClass : Arrays.asList(TestSparkApp.ClassicSpark.class, TestSparkApp.ScalaClassicSpark.class)) {
      final SparkManager sparkManager = appManager.getSparkManager(sparkClass.getSimpleName());
      sparkManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    }

    KeyValueTable resultTable = this.<KeyValueTable>getDataset("ResultTable").get();
    Assert.assertEquals(1L, Bytes.toLong(resultTable.read(ClassicSparkProgram.class.getName())));
    Assert.assertEquals(1L, Bytes.toLong(resultTable.read(ScalaClassicSparkProgram.class.getName())));
  }

  @Test
  public void testDynamicSpark() throws Exception {
    ApplicationManager appManager = deploy(TestSparkApp.class);

    // Write some data to a local file
    File inputFile = TEMP_FOLDER.newFile();
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile.toPath(), StandardCharsets.UTF_8)) {
      for (int i = 0; i < 10; i++) {
        writer.write("Line " + (i + 1));
        writer.newLine();
      }
    }

    SparkManager sparkManager = appManager.getSparkManager(ScalaDynamicSpark.class.getSimpleName());
    sparkManager.startAndWaitForRun(ImmutableMap.of("input", inputFile.getAbsolutePath(),
                                                    "output", "ResultTable",
                                                    "tmpdir", TMP_FOLDER.newFolder().getAbsolutePath()),
                                    ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // Validate the result written to dataset
    KeyValueTable resultTable = this.<KeyValueTable>getDataset("ResultTable").get();
    // There should be ten "Line"
    Assert.assertEquals(10, Bytes.toInt(resultTable.read("Line")));
    // Each number should appear once
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(1, Bytes.toInt(resultTable.read(Integer.toString(i + 1))));
    }
  }

  @Test
  public void testPySpark() throws Exception {
    ApplicationManager appManager = deploy(TestSparkApp.class);

    // Write some data to a local file
    File inputFile = TEMP_FOLDER.newFile();
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile.toPath(), StandardCharsets.UTF_8)) {
      for (int i = 0; i < 100; i++) {
        writer.write("Event " + i);
        writer.newLine();
      }
    }

    File outputDir = new File(TMP_FOLDER.newFolder(), "output");
    appManager.getSparkManager(PythonSpark.class.getSimpleName())
      .startAndWaitForRun(ImmutableMap.of("input.file", inputFile.getAbsolutePath(),
                                          "output.path", outputDir.getAbsolutePath()),
                          ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    // Verify the result
    File resultFile = Iterables.find(DirUtils.listFiles(outputDir), input ->
      !input.getName().endsWith(".crc") && !input.getName().startsWith("_SUCCESS"));


    List<String> lines = Files.readAllLines(resultFile.toPath(), StandardCharsets.UTF_8);
    Assert.assertTrue(!lines.isEmpty());

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
      Constants.Metrics.Tag.APP, TestSparkApp.class.getSimpleName(),
      Constants.Metrics.Tag.SPARK, PythonSpark.class.getSimpleName());

    Tasks.waitFor(100L, () ->  getMetricsManager().getTotalMetric(tags, "user.body"),
                  5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }


  @Test
  public void testSparkProgramStatusSchedule() throws Exception {
    ApplicationManager appManager = deploy(TestSparkApp.class);
    ScheduleId scheduleId = new ScheduleId(NamespaceId.DEFAULT.getNamespace(), TestSparkApp.class.getSimpleName(),
                                           "schedule");
    appManager.enableSchedule(scheduleId);

    WorkflowManager workflowManager =
      appManager.getWorkflowManager(TestSparkApp.TriggeredWorkflow.class.getSimpleName());
    int numRuns = workflowManager.getHistory(ProgramRunStatus.COMPLETED).size();

    // Start the upstream program
    SparkManager sparkManager = appManager.getSparkManager(TestSparkApp.ScalaClassicSpark.class.getSimpleName());
    sparkManager.start();

    // Wait for the downstream to complete
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // Run again with the kryo serializer
    sparkManager.start(Collections.singletonMap("spark.serializer", "org.apache.spark.serializer.KryoSerializer"));

    // Wait for the downstream to complete again
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, numRuns + 2, 5, TimeUnit.MINUTES);

  }

  @Test
  public void testTransaction() throws Exception {
    ApplicationManager applicationManager = deploy(TestSparkApp.class);

    // Write some data to a local file
    File inputFile = TEMP_FOLDER.newFile();
    try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(inputFile.toPath(), StandardCharsets.UTF_8))) {
      writer.println("red fox");
      writer.println("brown fox");
      writer.println("grey fox");
      writer.println("brown bear");
      writer.println("black bear");
    }

    // Run the spark program
    SparkManager sparkManager = applicationManager.getSparkManager(TransactionSpark.class.getSimpleName());
    sparkManager.start(ImmutableMap.of(
      "input.file", inputFile.getAbsolutePath(),
      "keyvalue.table", "KeyValueTable",
      "result.all.dataset", "SparkResult",
      "result.threshold", "2",
      "result.threshold.dataset", "SparkThresholdResult"
    ));

    // Verify result from dataset before the Spark program terminates
    final DataSetManager<KeyValueTable> resultManager = getDataset("SparkThresholdResult");
    final KeyValueTable resultTable = resultManager.get();

    // Expect the threshold result dataset, with threshold >=2, contains [brown, fox, bear]
    Tasks.waitFor(ImmutableSet.of("brown", "fox", "bear"), () -> {
      resultManager.flush();    // This is to start a new TX
      LOG.info("Reading from threshold result");
      try (CloseableIterator<KeyValue<byte[], byte[]>> itor = resultTable.scan(null, null)) {
        return ImmutableSet.copyOf(Iterators.transform(itor, input -> {
          String word = Bytes.toString(input.getKey());
          LOG.info("{}, {}", word, Bytes.toInt(input.getValue()));
          return word;
        }));
      }
    }, 3, TimeUnit.MINUTES, 1, TimeUnit.SECONDS);

    sparkManager.stop();
    sparkManager.waitForRun(ProgramRunStatus.KILLED, 60, TimeUnit.SECONDS);
  }

  @Test
  public void testSparkWithGetDataset() throws Exception {
    testSparkWithGetDataset(SparkAppUsingGetDataset.class, SparkLogParser.class.getSimpleName());
    testSparkWithGetDataset(SparkAppUsingGetDataset.class, ScalaSparkLogParser.class.getSimpleName());
  }

  @Test
  public void testSparkHttpService() throws Exception {
    ApplicationManager applicationManager = deploy(TestSparkApp.class);
    SparkManager sparkManager = applicationManager.getSparkManager(SparkServiceProgram.class.getSimpleName()).start();

    URL url = sparkManager.getServiceURL(5, TimeUnit.MINUTES);
    Assert.assertNotNull(url);

    // GET request to sum n numbers.
    URL sumURL = url.toURI().resolve("sum?n=" + Joiner.on("&n=").join(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).toURL();
    HttpURLConnection urlConn = (HttpURLConnection) sumURL.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, urlConn.getResponseCode());
    try (InputStream is = urlConn.getInputStream()) {
      Assert.assertEquals(55, Integer.parseInt(new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8)));
    }

    URL wordcountURL = url.toURI().resolve("wordcount").toURL();
    urlConn = (HttpURLConnection) wordcountURL.openConnection();

    // POST lines of sentences
    urlConn.setDoOutput(true);
    urlConn.setChunkedStreamingMode(10);

    List<String> messages = new ArrayList<>();
    try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(urlConn.getOutputStream(), "UTF-8"))) {
      for (int i = 0; i < 10; i++) {
        writer.printf("Message number %d\n", i);
        messages.add("Message number " + i);
      }
    }

    Assert.assertEquals(200, urlConn.getResponseCode());
    try (Reader reader = new InputStreamReader(urlConn.getInputStream(), "UTF-8")) {
      Map<String, Integer> result = new Gson().fromJson(reader, new TypeToken<Map<String, Integer>>() { }.getType());

      // Do a wordcount locally to get the expected result
      Map<String, Integer> expected = messages.stream()
        .flatMap((Function<String, Stream<String>>) s -> Arrays.stream(s.split("\\s+")))
        .map(s -> Maps.immutableEntry(s, 1))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1 + v2));

      Assert.assertEquals(expected, result);
    }

    sparkManager.stop();
  }

  @Test
  public void testSparkServicePlugin() throws Exception {
    addPluginArtifact(NamespaceId.DEFAULT.artifact("plugin", "1.0"), Collections.emptySet(), StringLengthFunc.class);

    // Generate some lines to a file
    File file = TMP_FOLDER.newFile();
    try (PrintStream printer = new PrintStream(file)) {
      for (int i = 0; i < 1000; i++) {
        printer.printf("Message %d\n", i);
      }
    }

    ApplicationManager applicationManager = deploy(TestSparkApp.class);
    SparkManager sparkManager = applicationManager.getSparkManager(SparkServiceProgram.class.getSimpleName()).start();

    URL url = sparkManager.getServiceURL(5, TimeUnit.MINUTES);
    Assert.assertNotNull(url);

    URL pluginURL = url.toURI().resolve("plugin?pluginType=function&pluginName=len&file="
                                          + URLEncoder.encode(file.getAbsolutePath(), "UTF-8")).toURL();
    HttpURLConnection urlConn = (HttpURLConnection) pluginURL.openConnection();
    Assert.assertEquals(200, urlConn.getResponseCode());

    try (Reader reader = new InputStreamReader(urlConn.getInputStream(), "UTF-8")) {
      Map<String, Integer> result = new Gson().fromJson(reader, new TypeToken<Map<String, Integer>>() { }.getType());
      // The result should be from each line in the file to the length of the line
      Assert.assertEquals(1000, result.size());
      Assert.assertTrue(Files.lines(file.toPath()).allMatch(line -> result.getOrDefault(line, -1) == line.length()));
    }

    // Deploy the UDT plugin and test the plugin extending plugin case
    addPluginArtifact(NamespaceId.DEFAULT.artifact("pluggable", "1.0"), Collections.emptySet(), PluggableFunc.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("lenudt", "1.0"), NamespaceId.DEFAULT.artifact("pluggable", "1.0"),
                      StringLengthUDT.class);

    pluginURL = url.toURI().resolve("udtPlugin?udtName=len&file="
                                          + URLEncoder.encode(file.getAbsolutePath(), "UTF-8")).toURL();
    urlConn = (HttpURLConnection) pluginURL.openConnection();
    Assert.assertEquals(200, urlConn.getResponseCode());

    try (Reader reader = new InputStreamReader(urlConn.getInputStream(), "UTF-8")) {
      Map<String, Integer> result = new Gson().fromJson(reader, new TypeToken<Map<String, Integer>>() { }.getType());
      // The result should be from each line in the file to the length of the line
      Assert.assertEquals(1000, result.size());
      Assert.assertTrue(Files.lines(file.toPath()).allMatch(line -> result.getOrDefault(line, -1) == line.length()));
    }
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

    SparkManager sparkManager = applicationManager.getSparkManager(sparkProgram);
    sparkManager.startAndWaitForRun(args, ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

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
