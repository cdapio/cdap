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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.batch.mock.ErrorTransform;
import co.cask.cdap.etl.batch.mock.MockSink;
import co.cask.cdap.etl.batch.mock.MockSource;
import co.cask.cdap.etl.batch.mock.StringValueFilterTransform;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for ETLBatch.
 */
public class ETLMapReduceTestRun extends ETLBatchTestBase {

  @Test
  public void testInvalidTransformConfigFailsToDeploy() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(new ETLStage("source", MockSource.getPlugin("inputTable")))
      .addSink(new ETLStage("sink", MockSink.getPlugin("outputTable")))
      .addTransform(new ETLStage("transform", StringValueFilterTransform.getPlugin(null, null)))
      .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "badConfig");
    try {
      deployApplication(appId, appRequest);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testAggregator() throws Exception {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(new ETLStage("source", MockSource.getPlugin("daginput")))
      .addSink(new ETLStage("sink", MockSink.getPlugin("dagoutput")))
      .addTransform(new ETLStage("filter", StringValueFilterTransform.getPlugin("name", "samuel")))
      .setAggregator(new ETLStage("agg", GroupByBatchAggregation.getPlugin(
        "name", ImmutableMap.of(
          "usd", new FunctionConfig("usd", SumAggregation.getPlugin())))))
      .addConnection("source", "filter")
      .addConnection("filter", "agg")
      .addConnection("agg", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "DagApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(schema).set("name", "jane").build();

    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, "daginput");
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob, recordJane));

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    // sink should only get non-samuel records
    DataSetManager<Table> sink1Manager = getDataset("dagoutput");
    Set<StructuredRecord> expected = ImmutableSet.of(recordBob, recordJane);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sink1Manager));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDAG() throws Exception {
    /*
     *            ----- error transform ------------
     *           |                                  |---- sink1
     * source --------- string value filter --------
     *           |                                  |---- sink2
     *            ----------------------------------
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(new ETLStage("source", MockSource.getPlugin("daginput")))
      .addSink(new ETLStage("sink1", MockSink.getPlugin("dagoutput1")))
      .addSink(new ETLStage("sink2", MockSink.getPlugin("dagoutput2")))
      .addTransform(new ETLStage("error", ErrorTransform.getPlugin(), "errors"))
      .addTransform(new ETLStage("filter", StringValueFilterTransform.getPlugin("name", "samuel")))
      .addConnection("source", "error")
      .addConnection("source", "filter")
      .addConnection("source", "sink2")
      .addConnection("error", "sink1")
      .addConnection("filter", "sink1")
      .addConnection("filter", "sink2")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "DagApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(schema).set("name", "jane").build();

    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, "daginput");
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob, recordJane));

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    // sink1 should only get non-samuel records
    DataSetManager<Table> sink1Manager = getDataset("dagoutput1");
    Set<StructuredRecord> expected = ImmutableSet.of(recordBob, recordJane);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sink1Manager));
    Assert.assertEquals(expected, actual);

    // sink2 should get bob and jane from the filter, plus everything again from the source
    Map<String, Integer> expectedCounts = ImmutableMap.of("samuel", 1, "bob", 2, "jane", 2);
    DataSetManager<Table> sink2Manager = getDataset("dagoutput2");
    Map<String, Integer> actualCounts = new HashMap<>();
    actualCounts.put("samuel", 0);
    actualCounts.put("bob", 0);
    actualCounts.put("jane", 0);
    for (StructuredRecord record : MockSink.readOutput(sink2Manager)) {
      String name = record.get("name");
      actualCounts.put(name, actualCounts.get(name) + 1);
    }
    Assert.assertEquals(expectedCounts, actualCounts);

    // error dataset should have all records
    Set<StructuredRecord> expectedErrors = ImmutableSet.of(recordSamuel, recordBob, recordJane);
    Set<StructuredRecord> actualErrors = new HashSet<>();
    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset("errors");
    try (TimePartitionedFileSet fileSet = fileSetManager.get()) {
      List<GenericRecord> records = readOutput(fileSet, Constants.ERROR_SCHEMA);
      for (GenericRecord record : records) {
        StructuredRecord invalidRecord = StructuredRecordStringConverter.fromJsonString(
          record.get(Constants.ErrorDataset.INVALIDENTRY).toString(), schema);
        actualErrors.add(invalidRecord);
      }
    }
    Assert.assertEquals(expectedErrors, actualErrors);
  }

}
