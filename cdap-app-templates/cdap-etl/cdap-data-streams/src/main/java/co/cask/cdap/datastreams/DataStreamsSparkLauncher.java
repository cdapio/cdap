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

package co.cask.cdap.datastreams;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.SparkConf;

import java.util.HashMap;
import java.util.Map;

/**
 * CDAP Spark client that configures and launches the actual Spark program.
 */
public class DataStreamsSparkLauncher extends AbstractSpark {
  public static final String NAME = "DataStreamsSparkStreaming";
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final DataStreamsPipelineSpec pipelineSpec;
  private final boolean isUnitTest;

  public DataStreamsSparkLauncher(DataStreamsPipelineSpec pipelineSpec, boolean isUnitTest) {
    this.pipelineSpec = pipelineSpec;
    this.isUnitTest = isUnitTest;
  }

  @Override
  protected void configure() {
    setName(NAME);
    setMainClass(SparkStreamingPipelineDriver.class);

    setExecutorResources(pipelineSpec.getResources());
    setDriverResources(pipelineSpec.getDriverResources());

    int numSources = 0;
    for (StageSpec stageSpec : pipelineSpec.getStages()) {
      if (StreamingSource.PLUGIN_TYPE.equals(stageSpec.getPlugin().getType())) {
        numSources++;
      }
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(pipelineSpec));
    properties.put("cask.hydrator.is.unit.test", String.valueOf(isUnitTest));
    properties.put("cask.hydrator.num.sources", String.valueOf(numSources));
    properties.put("cask.hydrator.extra.opts", pipelineSpec.getExtraJavaOpts());
    setProperties(properties);
  }

  @Override
  public void beforeSubmit(SparkClientContext context) throws Exception {
    SparkConf sparkConf = new SparkConf();
    // spark... makes you set this to at least the number of receivers (streaming sources)
    // because it holds one thread per receiver, or one core in distributed mode.
    // so... we have to set this hacky master variable based on the isUnitTest setting in the config
    Map<String, String> programProperties = context.getSpecification().getProperties();
    String extraOpts = programProperties.get("cask.hydrator.extra.opts");
    if (extraOpts != null && !extraOpts.isEmpty()) {
      sparkConf.set("spark.driver.extraJavaOptions", extraOpts);
      sparkConf.set("spark.executor.extraJavaOptions", extraOpts);
    }
    Boolean isUnitTest = Boolean.valueOf(programProperties.get("cask.hydrator.is.unit.test"));
    if (isUnitTest) {
      Integer numSources = Integer.valueOf(programProperties.get("cask.hydrator.num.sources"));
      sparkConf.setMaster(String.format("local[%d]", numSources + 1));
      // without this, stopping will hang on machines with few cores.
      sparkConf.set("spark.rpc.netty.dispatcher.numThreads", String.valueOf(numSources + 2));
    }
    context.setSparkConf(sparkConf);
  }

}
