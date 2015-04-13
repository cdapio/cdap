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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.templates.etl.batch.sinks.PartitionedFileSetSink;
import co.cask.cdap.templates.etl.batch.sources.PartitionedFileSetSource;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link PartitionedFileSetSource} and {@link PartitionedFileSetSink}
 */
public class ETLPartitionedFileSetSourceSinkTest extends TestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ETLPartitionedFileSetSourceSinkTest.class);
  private static final Gson GSON = new Gson();
  private static final String sourceFileset = "inputFileset";
  private static final String sinkFileset = "outputFileset";

  private static final String DUMMY_RECORD = "2015/3/10,Red Falcons,Blue Bonnets,28,17\n";

  @BeforeClass
  public static void beforeClass() throws Exception {
    addDatasetInstance("partitionedFileSet", sourceFileset, PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addStringField("league").addIntField("season").build())
      .setInputFormat(TextInputFormat.class)
      .build());

    addDatasetInstance("partitionedFileSet", sinkFileset, PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addStringField("league").build())
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ",")
      .build());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    clear();
  }

  @Test
  public void testConfig() throws Exception {

    writeToSource();

    ApplicationManager batchManager = deployApplication(ETLBatchTemplate.class);

    ApplicationTemplate<ETLBatchConfig> appTemplate = new ETLBatchTemplate();
    ETLBatchConfig adapterConfig = constructETLBatchConfig();
    MockAdapterConfigurer adapterConfigurer = new MockAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, adapterConfigurer);

    Map<String, String> mapReduceArgs = Maps.newHashMap();
    for (Map.Entry<String, String> entry : adapterConfigurer.getArguments().entrySet()) {
      mapReduceArgs.put(entry.getKey(), entry.getValue());
    }

    mapReduceArgs.put("config", GSON.toJson(adapterConfig));
    MapReduceManager mrManager = batchManager.startMapReduce("ETLMapReduce", mapReduceArgs);
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    batchManager.stopAll();

    verySinkData();
  }

  private void verySinkData() throws Exception {
    DataSetManager<PartitionedFileSet> sink = getDataset(sinkFileset);
    PartitionedFileSet sinkPFS = sink.get();
    Partition sinkPartition = sinkPFS.getPartition(PartitionKey.builder()
                                                     .addStringField("league", "nfl")
                                                     .build());

    Assert.assertNotNull("Partition not found", sinkPartition);
    try {
      Location location = sinkPartition.getLocation();
      for (Location file : location.list()) {
        if (file.getName().startsWith("part")) {
          BufferedReader reader = new BufferedReader(new InputStreamReader(location.getInputStream()));
          Assert.assertEquals("The data in source and sink is not same", reader.readLine(), DUMMY_RECORD);
        }
      }
    } catch (IOException e) {
      LOG.info("Unable to read path {}", sinkPartition.getRelativePath());
    }
  }

  private void writeToSource() throws Exception {
    DataSetManager<PartitionedFileSet> table1 = getDataset(sourceFileset);
    PartitionedFileSet inputFileset = table1.get();

    PartitionKey key = PartitionKey.builder()
      .addStringField("league", "nfl")
      .addIntField("season", 1985)
      .build();

    if (inputFileset.getPartition(key) != null) {
      LOG.info("Partition already exists");
      throw new RuntimeException("Partition already exists");
    }
    PartitionOutput output = inputFileset.getPartitionOutput(key);

    try {
      Location location = output.getLocation();
      OutputStream outputStream = location.getOutputStream();
      try {
        outputStream.write(DUMMY_RECORD.getBytes(Charsets.UTF_8));
      } finally {
        outputStream.close();
      }
    } catch (IOException e) {
      LOG.warn(String.format("Unable to write path '%s'", output.getRelativePath()));
      throw e;
    }
    output.addPartition();
    table1.flush();
  }

  private ETLBatchConfig constructETLBatchConfig() {
    ETLStage source = new ETLStage(PartitionedFileSetSource.class.getSimpleName(),
                                   ImmutableMap.of("filesetName", sourceFileset, "filter",
                                                   "{\"league.value\": \"nfl\",\"season.lower\": " +
                                                     "\"1980\",\"season.upper\": \"1990\"}"));
    ETLStage sink = new ETLStage(PartitionedFileSetSink.class.getSimpleName(),
                                 ImmutableMap.of("filesetName", sinkFileset, "outputPath", sinkFileset,
                                                 "partitionKey", "{\"league\" : \"nfl\"}"));
    List<ETLStage> transformList = Lists.newArrayList();
    return new ETLBatchConfig("", source, sink, transformList);
  }
}
