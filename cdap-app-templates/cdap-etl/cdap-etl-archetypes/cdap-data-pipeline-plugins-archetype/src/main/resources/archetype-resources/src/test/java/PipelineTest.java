/*
 * Copyright © 2016 Cask Data, Inc.
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

package $package;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for our plugins.
 */
public class PipelineTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      TextFileSetSource.class,
                      TextFileSetSink.class,
                      WordCountAggregator.class,
                      WordCountCompute.class,
                      WordCountSink.class);
  }

  @Test
  public void testTextFileSourceAndMoveAction() throws Exception {
    // create the pipeline config
    String moveFromName = "sourceTestMoveFrom";
    String inputName = "sourceTestInput";
    String outputName = "sourceTestOutput";

    Map<String, String> actionProperties = new HashMap<>();
    actionProperties.put(FilesetMoveAction.Conf.SOURCE_FILESET, "sourceTestMoveFrom");
    actionProperties.put(FilesetMoveAction.Conf.DEST_FILESET, inputName);
    ETLStage moveAction =
      new ETLStage("moveInput", new ETLPlugin(FilesetMoveAction.NAME, Action.PLUGIN_TYPE, actionProperties, null));

    Map<String, String> sourceProperties = new HashMap<>();
    sourceProperties.put(TextFileSetSource.Conf.FILESET_NAME, inputName);
    sourceProperties.put(TextFileSetSource.Conf.CREATE_IF_NOT_EXISTS, "true");
    sourceProperties.put(TextFileSetSource.Conf.DELETE_INPUT_ON_SUCCESS, "true");
    sourceProperties.put(TextFileSetSource.Conf.FILES, "${file}");
    ETLStage source =
      new ETLStage("source", new ETLPlugin(TextFileSetSource.NAME, BatchSource.PLUGIN_TYPE, sourceProperties, null));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputName));

    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(moveAction)
      .addConnection(moveAction.getName(), source.getName())
      .addConnection(source.getName(), sink.getName())
      .build();

    // create the move from fileset
    addDatasetInstance(FileSet.class.getName(), moveFromName);

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("textSourceTestPipeline");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));

    // write some files that will be moved to the input fileset
    DataSetManager<FileSet> moveFromManager = getDataset(moveFromName);

    // this file starts with '.' and should be ignored.
    Location invisibleFile = moveFromManager.get().getBaseLocation().append(".hidden");
    try (OutputStream outputStream = invisibleFile.getOutputStream()) {
      outputStream.write(Bytes.toBytes("this should not be read"));
    }

    // this file should be moved
    String line1 = "Hello World!";
    String line2 = "Good to meet you";
    String line3 = "My name is Hal";
    String inputText = line1 + "\n" + line2 + "\n" + line3;
    Location inputFile = moveFromManager.get().getBaseLocation().append("inputFile");
    try (OutputStream outputStream = inputFile.getOutputStream()) {
      outputStream.write(Bytes.toBytes(inputText));
    }

    // run the pipeline
    Map<String, String> runtimeArgs = new HashMap<>();
    // the ${file} macro will be substituted with "inputFile" for our pipeline run
    runtimeArgs.put("file", "inputFile");

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(runtimeArgs);
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    // check the pipeline output
    DataSetManager<Table> outputManager = getDataset(outputName);
    Set<StructuredRecord> outputRecords = new HashSet<>();
    outputRecords.addAll(MockSink.readOutput(outputManager));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(TextFileSetSource.OUTPUT_SCHEMA)
                   .set("position", (long) inputText.indexOf(line1))
                   .set("text", line1)
                   .build());
    expected.add(StructuredRecord.builder(TextFileSetSource.OUTPUT_SCHEMA)
                   .set("position", (long) inputText.indexOf(line2))
                   .set("text", line2)
                   .build());
    expected.add(StructuredRecord.builder(TextFileSetSource.OUTPUT_SCHEMA)
                   .set("position", (long) inputText.indexOf(line3))
                   .set("text", line3)
                   .build());
    Assert.assertEquals(expected, outputRecords);

    // check that the input file does not exist in the moveFrom fileSet,
    // and was deleted by the source in the input fileSet
    Assert.assertFalse(moveFromManager.get().getBaseLocation().append("inputFile").exists());
    DataSetManager<FileSet> inputManager = getDataset(inputName);
    Assert.assertFalse(inputManager.get().getBaseLocation().append("inputFile").exists());
  }

  @Test
  public void testTextFileSinkAndDeletePostAction() throws Exception {
    // create the pipeline config
    String inputName = "sinkTestInput";
    String outputName = "sinkTestOutput";
    String outputDirName = "users";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputName));

    Map<String, String> sinkProperties = new HashMap<>();
    sinkProperties.put(TextFileSetSink.Conf.FILESET_NAME, outputName);
    sinkProperties.put(TextFileSetSink.Conf.FIELD_SEPARATOR, "|");
    sinkProperties.put(TextFileSetSink.Conf.OUTPUT_DIR, "${dir}");
    ETLStage sink = new ETLStage("sink", new ETLPlugin(TextFileSetSink.NAME, BatchSink.PLUGIN_TYPE,
                                                       sinkProperties, null));

    Map<String, String> actionProperties = new HashMap<>();
    actionProperties.put(FilesetDeletePostAction.Conf.FILESET_NAME, outputName);
    // mapreduce writes multiple files to the output directory. Along with the actual output,
    // there are various .crc files that do not contain any of the output content.
    actionProperties.put(FilesetDeletePostAction.Conf.DELETE_REGEX, ".*\\.crc|_SUCCESS");
    actionProperties.put(FilesetDeletePostAction.Conf.DIRECTORY, outputDirName);
    ETLStage postAction = new ETLStage("cleanup", new ETLPlugin(FilesetDeletePostAction.NAME, PostAction.PLUGIN_TYPE,
                                                                actionProperties, null));

    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addPostAction(postAction)
      .addConnection(source.getName(), sink.getName())
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("textSinkTestPipeline");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));

    // write some data to the input fileset
    Schema inputSchema = Schema.recordOf("test",
                                         Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("item", Schema.of(Schema.Type.STRING)));

    Map<String, String> users = new HashMap<>();
    users.put("samuel", "wallet");
    users.put("dwayne", "rock");
    users.put("christopher", "cowbell");

    List<StructuredRecord> inputRecords = new ArrayList<>();
    for (Map.Entry<String, String> userEntry : users.entrySet()) {
      String name = userEntry.getKey();
      String item = userEntry.getValue();
      inputRecords.add(StructuredRecord.builder(inputSchema).set("name", name).set("item", item).build());
    }
    DataSetManager<Table> inputManager = getDataset(inputName);
    MockSource.writeInput(inputManager, inputRecords);

    // run the pipeline
    Map<String, String> runtimeArgs = new HashMap<>();
    // the ${dir} macro will be substituted with "users" for our pipeline run
    runtimeArgs.put("dir", outputDirName);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(runtimeArgs);
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    // check the pipeline output
    DataSetManager<FileSet> outputManager = getDataset(outputName);
    FileSet output = outputManager.get();
    Location outputDir = output.getBaseLocation().append(outputDirName);

    Map<String, String> actual = new HashMap<>();
    for (Location outputFile : outputDir.list()) {
      if (outputFile.getName().endsWith(".crc") || "_SUCCESS".equals(outputFile.getName())) {
        Assert.fail("Post action did not delete file " + outputFile.getName());
      }

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(outputFile.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] parts = line.split("\\|");
          actual.put(parts[0], parts[1]);
        }
      }
    }

    Assert.assertEquals(actual, users);
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testWordCountSparkSink() throws Exception {
    String inputName = "sparkSinkInput";
    String outputName = "sparkSinkOutput";

    // create the pipeline config
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputName));
    Map<String, String> sinkProperties = new HashMap<>();
    sinkProperties.put("field", "text");
    sinkProperties.put("tableName", outputName);
    ETLStage sink = new ETLStage("sink",
                                 new ETLPlugin(WordCountSink.NAME, SparkSink.PLUGIN_TYPE, sinkProperties, null));
    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("sparkSinkTestPipeline");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));

    // write the input
    Schema inputSchema = Schema.recordOf("text", Schema.Field.of("text", Schema.of(Schema.Type.STRING)));
    DataSetManager<Table> inputManager = getDataset(inputName);
    List<StructuredRecord> inputRecords = new ArrayList<>();
    inputRecords.add(StructuredRecord.builder(inputSchema).set("text", "Hello World").build());
    inputRecords.add(StructuredRecord.builder(inputSchema).set("text", "Hello my name is Hal").build());
    inputRecords.add(StructuredRecord.builder(inputSchema).set("text", "Hello my name is Sam").build());
    MockSource.writeInput(inputManager, inputRecords);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> outputManager = getDataset(outputName);
    KeyValueTable output = outputManager.get();
    Assert.assertEquals(3L, Bytes.toLong(output.read("Hello")));
    Assert.assertEquals(1L, Bytes.toLong(output.read("World")));
    Assert.assertEquals(2L, Bytes.toLong(output.read("my")));
    Assert.assertEquals(2L, Bytes.toLong(output.read("name")));
    Assert.assertEquals(2L, Bytes.toLong(output.read("is")));
    Assert.assertEquals(1L, Bytes.toLong(output.read("Hal")));
    Assert.assertEquals(1L, Bytes.toLong(output.read("Sam")));
  }

  @Test
  public void testStringCaseTransform() throws Exception {
    String inputName = "transformTestInput";
    String outputName = "transformTestOutput";

    // create the pipeline config
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputName));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputName));
    Map<String, String> transformProperties = new HashMap<>();
    transformProperties.put("lowerFields", "first");
    transformProperties.put("upperFields", "last");
    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin(StringCaseTransform.NAME, Transform.PLUGIN_TYPE,
                                                    transformProperties, null));
    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(transform)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("transformTestPipeline");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));

    // write the input
    Schema schema = Schema.recordOf(
      "name",
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING))
    );
    DataSetManager<Table> inputManager = getDataset(inputName);
    List<StructuredRecord> inputRecords = new ArrayList<>();
    inputRecords.add(StructuredRecord.builder(schema).set("first", "Samuel").set("last", "Jackson").build());
    MockSource.writeInput(inputManager, inputRecords);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    List<StructuredRecord> expected = new ArrayList<>();
    expected.add(StructuredRecord.builder(schema).set("first", "samuel").set("last", "JACKSON").build());
    Assert.assertEquals(expected, outputRecords);
  }

  @Test
  public void testWordCountAggregator() throws Exception {
    testWordCount(BatchAggregator.PLUGIN_TYPE);
  }

  @Test
  public void testWordCountSparkCompute() throws Exception {
    testWordCount(SparkCompute.PLUGIN_TYPE);
  }

  public void testWordCount(String pluginType) throws Exception {
    String inputName = "wcInput-" + pluginType;
    String outputName = "wcOutput-" + pluginType;

    // create the pipeline config
    ETLStage source = new ETLStage("wcInput", MockSource.getPlugin(inputName));
    ETLStage sink = new ETLStage("wcOutput", MockSink.getPlugin(outputName));
    Map<String, String> aggProperties = new HashMap<>();
    aggProperties.put("field", "text");
    ETLStage agg = new ETLStage("middle", new ETLPlugin("WordCount", pluginType, aggProperties, null));
    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(agg)
      .addConnection(source.getName(), agg.getName())
      .addConnection(agg.getName(), sink.getName())
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("wcTestPipeline-" + pluginType);
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));

    // write the input
    Schema inputSchema = Schema.recordOf("text", Schema.Field.of("text", Schema.of(Schema.Type.STRING)));
    DataSetManager<Table> inputManager = getDataset(inputName);
    List<StructuredRecord> inputRecords = new ArrayList<>();
    inputRecords.add(StructuredRecord.builder(inputSchema).set("text", "Hello World").build());
    inputRecords.add(StructuredRecord.builder(inputSchema).set("text", "Hello my name is Hal").build());
    inputRecords.add(StructuredRecord.builder(inputSchema).set("text", "Hello my name is Sam").build());
    MockSource.writeInput(inputManager, inputRecords);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputName);
    Set<StructuredRecord> outputRecords = new HashSet<>();
    outputRecords.addAll(MockSink.readOutput(outputManager));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(WordCountAggregator.OUTPUT_SCHEMA)
                   .set("word", "Hello")
                   .set("count", 3L).build());
    expected.add(StructuredRecord.builder(WordCountAggregator.OUTPUT_SCHEMA)
                   .set("word", "World")
                   .set("count", 1L).build());
    expected.add(StructuredRecord.builder(WordCountAggregator.OUTPUT_SCHEMA)
                   .set("word", "my")
                   .set("count", 2L).build());
    expected.add(StructuredRecord.builder(WordCountAggregator.OUTPUT_SCHEMA)
                   .set("word", "name")
                   .set("count", 2L).build());
    expected.add(StructuredRecord.builder(WordCountAggregator.OUTPUT_SCHEMA)
                   .set("word", "is")
                   .set("count", 2L).build());
    expected.add(StructuredRecord.builder(WordCountAggregator.OUTPUT_SCHEMA)
                   .set("word", "Hal")
                   .set("count", 1L).build());
    expected.add(StructuredRecord.builder(WordCountAggregator.OUTPUT_SCHEMA)
                   .set("word", "Sam")
                   .set("count", 1L).build());
    Assert.assertEquals(expected, outputRecords);
  }
}
