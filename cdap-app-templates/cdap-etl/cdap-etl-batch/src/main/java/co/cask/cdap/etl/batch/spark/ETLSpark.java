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

package co.cask.cdap.etl.batch.spark;

import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.batch.AbstractAggregatorContext;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.CompositeFinisher;
import co.cask.cdap.etl.batch.Finisher;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.base.Joiner;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Configures and sets up runs of {@link ETLSparkProgram}.
 */
public class ETLSpark extends AbstractSpark {
  private static final Logger LOG = LoggerFactory.getLogger(ETLSpark.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>()).create();

  private final BatchPhaseSpec phaseSpec;
  private Finisher finisher;
  private List<File> cleanupFiles;

  public ETLSpark(BatchPhaseSpec phaseSpec) {
    this.phaseSpec = phaseSpec;
  }

  @Override
  protected void configure() {
    setName(phaseSpec.getPhaseName());
    setDescription("Spark phase executor. " + phaseSpec.getDescription());

    setMainClass(ETLSparkProgram.class);

    setExecutorResources(phaseSpec.getResources());
    setDriverResources(phaseSpec.getResources());

    if (phaseSpec.getPhase().getSources().size() != 1) {
      throw new IllegalArgumentException("Pipeline must contain exactly one source.");
    }
    if (phaseSpec.getPhase().getSinks().isEmpty()) {
      throw new IllegalArgumentException("Pipeline must contain at least one sink.");
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(phaseSpec));
    setProperties(properties);
  }

  @Override
  public void beforeSubmit(SparkClientContext context) throws Exception {
    cleanupFiles = new ArrayList<>();
    CompositeFinisher.Builder finishers = CompositeFinisher.builder();

    context.setSparkConf(new SparkConf().set("spark.driver.extraJavaOptions", "-XX:MaxPermSize=256m"));
    Map<String, String> properties = context.getSpecification().getProperties();
    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);
    PipelinePluginInstantiator pluginInstantiator =
      new PipelinePluginInstantiator(context, phaseSpec);
    // we checked at configure time that there is exactly one source
    String sourceName = phaseSpec.getPhase().getSources().iterator().next();

    BatchConfigurable<BatchSourceContext> batchSource = pluginInstantiator.newPluginInstance(sourceName);
    DatasetContextLookupProvider lookProvider = new DatasetContextLookupProvider(context);
    SparkBatchSourceContext sourceContext = new SparkBatchSourceContext(context,
                                                                        lookProvider,
                                                                        sourceName);
    batchSource.prepareRun(sourceContext);

    SparkBatchSourceFactory sourceFactory = sourceContext.getSourceFactory();
    if (sourceFactory == null) {
      // TODO: Revisit what exception to throw
      throw new IllegalArgumentException("No input was set. Please make sure the source plugin calls setInput when " +
                                           "preparing the run.");
    }
    finishers.add(batchSource, sourceContext);

    SparkBatchSinkFactory sinkFactory = new SparkBatchSinkFactory();
    for (String sinkName : phaseSpec.getPhase().getSinks()) {
      BatchConfigurable<BatchSinkContext> batchSink = pluginInstantiator.newPluginInstance(sinkName);
      if (batchSink instanceof SparkSink) {
        BasicSparkPluginContext sparkPluginContext = new BasicSparkPluginContext(context, lookProvider, sinkName);
        ((SparkSink) batchSink).prepareRun(sparkPluginContext);
        finishers.add((SparkSink) batchSink, sparkPluginContext);
      } else {
        BatchSinkContext sinkContext = new SparkBatchSinkContext(sinkFactory, context, null, sinkName);
        batchSink.prepareRun(sinkContext);
        finishers.add(batchSink, sinkContext);
      }
    }

    Set<StageInfo> aggregators = phaseSpec.getPhase().getStagesOfType(BatchAggregator.PLUGIN_TYPE);
    Integer numPartitions = null;
    if (!aggregators.isEmpty()) {
      if (aggregators.size() > 1) {
        throw new IllegalArgumentException(String.format(
          "There was an error during planning. Phase %s has multiple aggregators %s.",
          phaseSpec.getPhaseName(), Joiner.on(',').join(aggregators)));
      }
      String aggregatorName = aggregators.iterator().next().getName();
      BatchAggregator aggregator = pluginInstantiator.newPluginInstance(aggregatorName);
      AbstractAggregatorContext aggregatorContext =
        new SparkAggregatorContext(context, new DatasetContextLookupProvider(context), aggregatorName);
      aggregator.prepareRun(aggregatorContext);
      finishers.add(aggregator, aggregatorContext);
      numPartitions = aggregatorContext.getNumPartitions();
    }

    File configFile = File.createTempFile("ETLSpark", ".config");
    cleanupFiles.add(configFile);
    try (OutputStream os = new FileOutputStream(configFile)) {
      sourceFactory.serialize(os);
      sinkFactory.serialize(os);
      DataOutput dataOutput = new DataOutputStream(os);
      dataOutput.writeInt(numPartitions == null ? -1 : numPartitions);
    }

    finisher = finishers.build();
    context.localize("ETLSpark.config", configFile.toURI());
  }

  @Override
  public void onFinish(boolean succeeded, SparkClientContext context) throws Exception {
    finisher.onFinish(succeeded);
    for (File file : cleanupFiles) {
      if (!file.delete()) {
        LOG.warn("Failed to clean up resource {} ", file);
      }
    }
  }

}
