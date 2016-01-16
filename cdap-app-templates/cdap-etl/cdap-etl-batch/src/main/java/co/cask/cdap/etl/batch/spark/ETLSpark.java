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

package co.cask.cdap.etl.batch.spark;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.Pipeline;
import co.cask.cdap.etl.common.PipelineRegisterer;
import co.cask.cdap.etl.common.SinkInfo;
import com.google.gson.Gson;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configures and sets up runs of {@link ETLSparkProgram}.
 */
public class ETLSpark extends AbstractSpark {
  private static final Logger LOG = LoggerFactory.getLogger(ETLSpark.class);
  private static final Gson GSON = new Gson();

  private final ETLBatchConfig config;

  private List<Finisher> finishers;
  private List<File> cleanupFiles;

  public ETLSpark(ETLBatchConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    setDescription("Spark Driver for ETL Batch Applications");
    setMainClass(ETLSparkProgram.class);

    PipelineRegisterer pipelineRegisterer = new PipelineRegisterer(getConfigurer(), "batch");

    Pipeline pipelineIds =
      pipelineRegisterer.registerPlugins(
        config, TimePartitionedFileSet.class,
        FileSetProperties.builder()
          .setInputFormat(AvroKeyInputFormat.class)
          .setOutputFormat(AvroKeyOutputFormat.class)
          .setEnableExploreOnCreate(true)
          .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
          .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
          .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
          .setTableProperty("avro.schema.literal", Constants.ERROR_SCHEMA.toString())
          .build(), true);

    Resources resources = config.getResources();
    if (resources != null) {
      setExecutorResources(resources);
    }
    Resources driverResources = config.getDriverResources();
    if (driverResources != null) {
      setDriverResources(driverResources);
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(pipelineIds));
    setProperties(properties);
  }

  @Override
  public void beforeSubmit(SparkContext context) throws Exception {
    cleanupFiles = new ArrayList<>();
    finishers = new ArrayList<>();

    Map<String, String> properties = context.getSpecification().getProperties();
    Pipeline pipeline = GSON.fromJson(properties.get(Constants.PIPELINEID), Pipeline.class);
    String sourcePluginId = pipeline.getSource();

    PluginContext pluginContext = context.getPluginContext();
    BatchConfigurable<BatchSourceContext> batchSource = pluginContext.newPluginInstance(sourcePluginId);
    SparkBatchSourceContext sourceContext = new SparkBatchSourceContext(context,
                                                                        new DatasetContextLookupProvider(context),
                                                                        sourcePluginId);
    batchSource.prepareRun(sourceContext);

    SparkBatchSourceFactory sourceFactory = sourceContext.getSourceFactory();
    if (sourceFactory == null) {
      // TODO: Revisit what exception to throw
      throw new IllegalArgumentException("No input was set. Please make sure the source plugin calls setInput when " +
                                           "preparing the run.");
    }
    addFinisher(batchSource, sourceContext, finishers);

    SparkBatchSinkFactory sinkFactory = new SparkBatchSinkFactory();
    List<SinkInfo> sinkInfos = pipeline.getSinks();
    for (SinkInfo sinkInfo : sinkInfos) {
      BatchConfigurable<BatchSinkContext> batchSink = pluginContext.newPluginInstance(sinkInfo.getSinkId());
      BatchSinkContext sinkContext = new SparkBatchSinkContext(sinkFactory, context, null, sinkInfo.getSinkId());
      batchSink.prepareRun(sinkContext);
      addFinisher(batchSink, sinkContext, finishers);
    }

    File configFile = File.createTempFile("ETLSpark", ".config");
    cleanupFiles.add(configFile);
    try (OutputStream os = new FileOutputStream(configFile)) {
      sourceFactory.serialize(os);
      sinkFactory.serialize(os);
    }

    context.localize("ETLSpark.config", configFile.toURI());
  }

  @Override
  public void onFinish(boolean succeeded, SparkContext context) throws Exception {
    for (Finisher finisher : finishers) {
      finisher.onFinish(succeeded);
    }
    for (File file : cleanupFiles) {
      if (!file.delete()) {
        LOG.warn("Failed to clean up resource {} ", file);
      }
    }
  }

  private <T extends BatchContext> void addFinisher(final BatchConfigurable<T> configurable,
                                                    final T context, List<Finisher> finishers) {
    finishers.add(new Finisher() {
      @Override
      public void onFinish(boolean succeeded) {
        configurable.onRunFinish(succeeded, context);
      }
    });
  }

  private interface Finisher {
    void onFinish(boolean succeeded);
  }
}
