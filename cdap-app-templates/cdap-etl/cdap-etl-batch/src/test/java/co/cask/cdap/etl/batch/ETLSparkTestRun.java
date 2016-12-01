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
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.transform.StringValueFilterTransform;
import co.cask.cdap.etl.proto.Engine;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ETLSparkTestRun extends ETLBatchTestBase {

  @Test
  public void test() throws Exception {
    /*
     *            ---------------- sink1
     *           |
     * source ---
     *           |
     *            ---- filter ---- sink2
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setEngine(Engine.SPARK)
      .addStage(new ETLStage("source", MockSource.getPlugin("sparkinput")))
      .addStage(new ETLStage("sink1", MockSink.getPlugin("sparkoutput1")))
      .addStage(new ETLStage("sink2", MockSink.getPlugin("sparkoutput2")))
      .addStage(new ETLStage("filter", StringValueFilterTransform.getPlugin("name", "samuel")))
      .addConnection("source", "sink1")
      .addConnection("source", "filter")
      .addConnection("filter", "sink2")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("DagApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // write input
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(schema).set("name", "jane").build();

    DataSetManager<Table> sourceManager = getDataset(NamespaceId.DEFAULT.dataset("sparkinput"));
    MockSource.writeInput(sourceManager, ImmutableList.of(recordSamuel, recordBob, recordJane));

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    // check output of the first output, which was transformed before it was written
    DataSetManager<Table> sink1Manager = getDataset("sparkoutput1");
    Set<StructuredRecord> actualOutput = Sets.newHashSet(MockSink.readOutput(sink1Manager));
    Set<StructuredRecord> expectedOutput = ImmutableSet.of(recordSamuel, recordBob, recordJane);
    Assert.assertEquals(expectedOutput, actualOutput);

    // check output of the second output, which had one record filtered before it was written
    DataSetManager<Table> sink2Manager = getDataset("sparkoutput2");
    actualOutput = Sets.newHashSet(MockSink.readOutput(sink2Manager));
    expectedOutput = ImmutableSet.of(recordBob, recordJane);
    Assert.assertEquals(expectedOutput, actualOutput);
  }
}
