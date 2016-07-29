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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.spark.SparkCollection;
import co.cask.cdap.etl.spark.SparkPipelineDriver;
import co.cask.cdap.etl.spark.function.BatchSourceFunction;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Batch Spark pipeline driver.
 */
public class BatchSparkPipelineDriver extends SparkPipelineDriver
  implements JavaSparkMain, TxRunnable {
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private transient JavaSparkContext jsc;
  private transient JavaSparkExecutionContext sec;
  private transient SparkBatchSourceFactory sourceFactory;
  private transient SparkBatchSinkFactory sinkFactory;
  private transient DatasetContext datasetContext;
  private transient Map<String, Integer> stagePartitions;

  @Override
  protected SparkCollection<Object> getSource(String stageName, PluginFunctionContext pluginFunctionContext) {
    return new RDDCollection<>(sec, jsc, datasetContext, sinkFactory,
                               sourceFactory.createRDD(sec, jsc, stageName, Object.class, Object.class)
                                 .flatMap(new BatchSourceFunction(pluginFunctionContext)));
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    this.jsc = new JavaSparkContext();
    this.sec = sec;

    // Execution the whole pipeline in one long transaction. This is because the Spark execution
    // currently share the same contract and API as the MapReduce one.
    // The API need to expose DatasetContext, hence it needs to be executed inside a transaction
    sec.execute(this);
  }

  @Override
  public void run(DatasetContext context) throws Exception {
    BatchPhaseSpec phaseSpec = GSON.fromJson(sec.getSpecification().getProperty(Constants.PIPELINEID),
                                             BatchPhaseSpec.class);

    try (InputStream is = new FileInputStream(sec.getLocalizationContext().getLocalFile("HydratorSpark.config"))) {
      sourceFactory = SparkBatchSourceFactory.deserialize(is);
      sinkFactory = SparkBatchSinkFactory.deserialize(is);
      DataInputStream dataInputStream = new DataInputStream(is);
      stagePartitions = GSON.fromJson(dataInputStream.readUTF(), MAP_TYPE);
    }
    datasetContext = context;
    runPipeline(phaseSpec.getPhase(), BatchSource.PLUGIN_TYPE, sec, stagePartitions);
  }
}
