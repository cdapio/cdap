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

package co.cask.cdap.etl.realtime;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.etl.mock.realtime.LookupSource;
import co.cask.cdap.etl.mock.realtime.MockSink;
import co.cask.cdap.etl.mock.realtime.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.mock.transform.DoubleTransform;
import co.cask.cdap.etl.mock.transform.IdentityTransform;
import co.cask.cdap.etl.mock.transform.IntValueFilterTransform;
import co.cask.cdap.etl.proto.v2.ETLRealtimeConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for {@link ETLWorker}.
 */
public class ETLWorkerTest extends HydratorTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ETLWorkerTest.class);
  protected static final ArtifactId APP_ARTIFACT_ID =
    new ArtifactId(NamespaceId.DEFAULT.getNamespace(), "app", "1.0.0");
  protected static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");
  private static int startCount = 0;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @BeforeClass
  public static void setupTests() throws Exception {
    if (startCount++ > 0) {
      return;
    }
    setupRealtimeArtifacts(APP_ARTIFACT_ID, ETLRealtimeApplication.class);
  }

  @After
  public void cleanupTest() throws Exception {
    getMetricsManager().resetAll();
  }

  @Test
  @Category(SlowTests.class)
  public void testOneSourceOneSink() throws Exception {
    Schema schema = Schema.recordOf(
      "test",
      Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    List<StructuredRecord> input = new ArrayList<>();
    input.add(StructuredRecord.builder(schema).set("id", "123").set("name", "samuel").build());
    input.add(StructuredRecord.builder(schema).set("id", "456").set("name", "jackson").build());

    File tmpDir = TMP_FOLDER.newFolder();
    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(input)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(tmpDir)))
      .addConnection("source", "sink")
      .build();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "simpleApp");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);
    workerManager.start();
    workerManager.waitForStatus(true, 10, 1);

    try {
      List<StructuredRecord> written = MockSink.getRecords(tmpDir, 0, 10, TimeUnit.SECONDS);
      Assert.assertEquals(input, written);
    } finally {
      stopWorker(workerManager);
    }

    validateMetric(2, appId, "source.records.out");
    validateMetric(2, appId, "sink.records.in");
  }

  @Test
  public void testEmptyProperties() throws Exception {
    // Set properties to null to test if ETLTemplate can handle it.
    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(null)))
      .addConnection("source", "sink")
      .setInstances(2)
      .build();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "emptyTest");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    Assert.assertNotNull(appManager);
    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);
    workerManager.start();
    workerManager.waitForStatus(true, 10, 1);

    try {
      Assert.assertEquals(2, workerManager.getInstances());
    } finally {
      stopWorker(workerManager);
    }
  }

  @Test
  public void testLookup() throws Exception {
    addDatasetInstance(KeyValueTable.class.getName(), "lookupTable");
    DataSetManager<KeyValueTable> lookupTable = getDataset("lookupTable");
    lookupTable.get().write("Bob".getBytes(Charsets.UTF_8), "123".getBytes(Charsets.UTF_8));
    lookupTable.flush();

    File outDir = TMP_FOLDER.newFolder();
    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(new ETLStage("source", LookupSource.getPlugin(ImmutableSet.of("Bob", "Bill"), "lookupTable")))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outDir)))
      .addConnection("source", "sink")
      .build();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "lookupTestApp");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);
    workerManager.start();
    workerManager.waitForStatus(true, 10, 1);

    Schema schema = Schema.recordOf(
      "bobbill",
      Schema.Field.of("Bob", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("Bill", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );
    List<StructuredRecord> expected = new ArrayList<>();
    expected.add(StructuredRecord.builder(schema).set("Bob", "123").build());
    try {
      List<StructuredRecord> actual = MockSink.getRecords(outDir, 0, 10, TimeUnit.SECONDS);
      Assert.assertEquals(expected, actual);
    } finally {
      stopWorker(workerManager);
    }
    validateMetric(1, appId, "source.records.out");
    validateMetric(1, appId, "sink.records.in");
  }

  @Test
  public void testDAG() throws Exception {
    Schema schema = Schema.recordOf("testRecord", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    StructuredRecord record1 = StructuredRecord.builder(schema).set("x", 1).build();
    StructuredRecord record2 = StructuredRecord.builder(schema).set("x", 2).build();
    StructuredRecord record3 = StructuredRecord.builder(schema).set("x", 3).build();
    List<StructuredRecord> input = ImmutableList.of(record1, record2, record3);

    /*
     *            ----- value filter ------- sink1
     *           |
     * source --------- double --------
     *           |                     |---- sink2
     *            ----- identity ------
     */
    File sink1Out = TMP_FOLDER.newFolder();
    File sink2Out = TMP_FOLDER.newFolder();

    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(input)))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1Out)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2Out)))
      .addStage(new ETLStage("valueFilter", IntValueFilterTransform.getPlugin("x", 2)))
      .addStage(new ETLStage("double", DoubleTransform.getPlugin()))
      .addStage(new ETLStage("identity", IdentityTransform.getPlugin()))
      .addConnection("source", "valueFilter")
      .addConnection("source", "double")
      .addConnection("source", "identity")
      .addConnection("valueFilter", "sink1")
      .addConnection("double", "sink2")
      .addConnection("identity", "sink2")
      .build();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "dagTest");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    Assert.assertNotNull(appManager);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);
    workerManager.start();
    workerManager.waitForStatus(true, 10, 1);

    try {
      List<StructuredRecord> sink1output = MockSink.getRecords(sink1Out, 0, 10, TimeUnit.SECONDS);
      List<StructuredRecord> sink1expected = ImmutableList.of(record1, record3);
      Assert.assertEquals(sink1expected, sink1output);

      List<StructuredRecord> sink2output = MockSink.getRecords(sink2Out, 0, 10, TimeUnit.SECONDS);
      Assert.assertEquals(9, sink2output.size());
    } finally {
      stopWorker(workerManager);
    }

    validateMetric(3, appId, "source.records.out");
    validateMetric(3, appId, "valueFilter.records.in");
    validateMetric(2, appId, "valueFilter.records.out");
    validateMetric(3, appId, "double.records.in");
    validateMetric(6, appId, "double.records.out");
    validateMetric(3, appId, "identity.records.in");
    validateMetric(3, appId, "identity.records.out");
    validateMetric(2, appId, "sink1.records.in");
    validateMetric(9, appId, "sink2.records.in");
  }

  private void stopWorker(WorkerManager workerManager) {
    try {
      workerManager.stop();
      workerManager.waitForStatus(false, 10, 1);
    } catch (Throwable t) {
      LOG.error("Error stopping worker.", t);
    }
  }

  private void validateMetric(long expected, Id.Application appId,
                              String metric) throws TimeoutException, InterruptedException {
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespaceId(),
                                               Constants.Metrics.Tag.APP, appId.getId(),
                                               Constants.Metrics.Tag.WORKER, ETLWorker.NAME);
    getMetricsManager().waitForTotalMetricCount(tags, "user." + metric, expected, 20, TimeUnit.SECONDS);
    // wait for won't throw an exception if the metric count is greater than expected
    Assert.assertEquals(expected, getMetricsManager().getTotalMetric(tags, "user." + metric));
  }
}
