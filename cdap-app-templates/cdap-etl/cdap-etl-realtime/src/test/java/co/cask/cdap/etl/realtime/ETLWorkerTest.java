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

package co.cask.cdap.etl.realtime;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.etl.common.Connection;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.etl.realtime.mock.DoubleTransform;
import co.cask.cdap.etl.realtime.mock.IdentityTransform;
import co.cask.cdap.etl.realtime.mock.IntValueFilterTransform;
import co.cask.cdap.etl.realtime.mock.LookupSource;
import co.cask.cdap.etl.realtime.mock.MockSink;
import co.cask.cdap.etl.realtime.mock.MockSource;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ETLWorker}.
 */
public class ETLWorkerTest extends ETLRealtimeBaseTest {
  private static final Logger LOG = LoggerFactory.getLogger(ETLWorkerTest.class);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

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
    ETLStage source = new ETLStage("source", MockSource.getPlugin(input));

    File tmpDir = TMP_FOLDER.newFolder();
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(tmpDir));
    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, ImmutableList.of(sink),
                                                        new ArrayList<ETLStage>(),
                                                        new ArrayList<Connection>());

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testToStream");
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
  }

  @Test
  public void testEmptyProperties() throws Exception {
    // Set properties to null to test if ETLTemplate can handle it.
    ETLStage source = new ETLStage("source", MockSource.getPlugin(null));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(null));
    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(2, source, sink, new ArrayList<ETLStage>());

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
    ETLStage source = new ETLStage("source", LookupSource.getPlugin(ImmutableSet.of("Bob", "Bill"), "lookupTable"));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outDir));
    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sink, new ArrayList<ETLStage>());

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testToStream");
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
    ETLStage source = new ETLStage("source", MockSource.getPlugin(input));

    List<ETLStage> transforms = ImmutableList.of(
      new ETLStage("valueFilter", IntValueFilterTransform.getPlugin("x", 2)),
      new ETLStage("double", DoubleTransform.getPlugin()),
      new ETLStage("identity", IdentityTransform.getPlugin())
    );

    File sink1Out = TMP_FOLDER.newFolder();
    File sink2Out = TMP_FOLDER.newFolder();
    List<ETLStage> sinks = ImmutableList.of(
      new ETLStage("sink1", MockSink.getPlugin(sink1Out)),
      new ETLStage("sink2", MockSink.getPlugin(sink2Out))
    );

    List<Connection> connections = ImmutableList.of(
      new Connection("source", "valueFilter"),
      new Connection("source", "double"),
      new Connection("source", "identity"),
      new Connection("valueFilter", "sink1"),
      new Connection("double", "sink2"),
      new Connection("identity", "sink2")
    );
    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sinks, transforms, connections);

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
  }

  private void stopWorker(WorkerManager workerManager) {
    try {
      workerManager.stop();
      workerManager.waitForStatus(false, 10, 1);
    } catch (Throwable t) {
      LOG.error("Error stopping worker.", t);
    }
  }
}
