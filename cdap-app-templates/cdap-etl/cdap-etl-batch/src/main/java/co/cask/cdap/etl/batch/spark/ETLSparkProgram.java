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

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.Pipeline;
import co.cask.cdap.etl.common.SinkInfo;
import co.cask.cdap.etl.common.TransformDetail;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.common.TransformInfo;
import co.cask.cdap.etl.common.TransformResponse;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Spark program to run an ETL pipeline.
 */
public class ETLSparkProgram implements JavaSparkProgram {

  private static final Logger LOG = LoggerFactory.getLogger(ETLSparkProgram.class);

  private static final Gson GSON = new Gson();


  @Override
  public void run(SparkContext context) throws Exception {
    SparkBatchSourceFactory sourceFactory;
    SparkBatchSinkFactory sinkFactory;
    try (InputStream is = new FileInputStream(context.getTaskLocalizationContext().getLocalFile("ETLSpark.config"))) {
      sourceFactory = SparkBatchSourceFactory.deserialize(is);
      sinkFactory = SparkBatchSinkFactory.deserialize(is);
    }

    JavaPairRDD<Object, Object> rdd = sourceFactory.createRDD(context, Object.class, Object.class);
    JavaPairRDD<String, Object> resultRDD = rdd.flatMapToPair(new MapFunction(context)).cache();

    Pipeline pipeline = GSON.fromJson(context.getSpecification().getProperty(Constants.PIPELINEID), Pipeline.class);
    for (SinkInfo sinkInfo : pipeline.getSinks()) {
      final String sinkId = sinkInfo.getSinkId();

      JavaPairRDD<Object, Object> sinkRDD = resultRDD
        .filter(new Function<Tuple2<String, Object>, Boolean>() {
          @Override
          public Boolean call(Tuple2<String, Object> v1) throws Exception {
            return v1._1().equals(sinkId);
          }
        })
        .flatMapToPair(new PairFlatMapFunction<Tuple2<String, Object>, Object, Object>() {
          @Override
          public Iterable<Tuple2<Object, Object>> call(Tuple2<String, Object> input) throws Exception {
            List<Tuple2<Object, Object>> result = new ArrayList<>();
            KeyValue<Object, Object> keyValue = (KeyValue<Object, Object>) input._2();
            result.add(new Tuple2<>(keyValue.getKey(), keyValue.getValue()));
            return result;
          }
        });
      sinkFactory.writeFromRDD(sinkRDD, context, sinkId, Object.class, Object.class);
    }
  }

  /**
   * Performs all transforms, and returns tuples where the first item is the sink to write to, and the second item
   * is the KeyValue to write.
   */
  public static final class MapFunction
    implements PairFlatMapFunction<Tuple2<Object, Object>, String, Object> {

    private final PluginContext pluginContext;
    private final Metrics metrics;
    private final long logicalStartTime;
    private final String pipelineStr;
    private final Map<String, String> runtimeArgs;
    private transient TransformExecutor<KeyValue<Object, Object>> transformExecutor;

    public MapFunction(SparkContext sparkContext) {
      this.pluginContext = sparkContext.getPluginContext();
      this.metrics = sparkContext.getMetrics();
      this.logicalStartTime = sparkContext.getLogicalStartTime();
      this.pipelineStr = sparkContext.getSpecification().getProperty(Constants.PIPELINEID);
      this.runtimeArgs = sparkContext.getRuntimeArguments();
    }

    @Override
    public Iterable<Tuple2<String, Object>> call(Tuple2<Object, Object> tuple) throws Exception {
      if (transformExecutor == null) {
        // TODO: There is no way to call destroy() method on Transform
        // In fact, we can structure transform in a way that it doesn't need destroy
        // All current usage of destroy() in transform is actually for Source/Sink, which is actually
        // better do it in prepareRun and onRunFinish, which happen outside of the Job execution (true for both
        // Spark and MapReduce).
        transformExecutor = initialize();
      }
      TransformResponse response = transformExecutor.runOneIteration(new KeyValue<>(tuple._1(), tuple._2()));

      List<Tuple2<String, Object>> result = new ArrayList<>();
      for (Map.Entry<String, Collection<Object>> transformedEntry : response.getSinksResults().entrySet()) {
        String sinkName = transformedEntry.getKey();
        for (Object outputRecord : transformedEntry.getValue()) {
          result.add(new Tuple2<>(sinkName, outputRecord));
        }
      }
      return result;
    }

    private TransformExecutor<KeyValue<Object, Object>> initialize() throws Exception {
      Pipeline pipeline = GSON.fromJson(pipelineStr, Pipeline.class);
      Map<String, List<String>> connections = pipeline.getConnections();
      // get source, transform, sink ids from program properties
      String sourcePluginId = pipeline.getSource();
      BatchSource source = pluginContext.newPluginInstance(sourcePluginId);
      BatchRuntimeContext runtimeContext = new SparkBatchRuntimeContext(pluginContext, metrics, logicalStartTime,
                                                                        runtimeArgs, sourcePluginId);
      source.initialize(runtimeContext);

      Map<String, TransformDetail> transformations = new HashMap<>();
      transformations.put(sourcePluginId, new TransformDetail(
        source, new DefaultStageMetrics(metrics, sourcePluginId), connections.get(sourcePluginId)));
      addTransforms(transformations, pipeline.getTransforms(), connections);

      List<SinkInfo> sinkInfos = pipeline.getSinks();
      for (SinkInfo sinkInfo : sinkInfos) {
        String sinkId = sinkInfo.getSinkId();
        BatchSink<Object, Object, Object> batchSink = pluginContext.newPluginInstance(sinkId);
        BatchRuntimeContext sinkContext = new SparkBatchRuntimeContext(pluginContext, metrics, logicalStartTime,
                                                                       runtimeArgs, sinkId);
        batchSink.initialize(sinkContext);
        transformations.put(sinkInfo.getSinkId(), new TransformDetail(
          batchSink, new DefaultStageMetrics(metrics, sinkInfo.getSinkId()), new ArrayList<String>()));
      }

      return new TransformExecutor<>(transformations, ImmutableList.of(sourcePluginId));
    }

    private void addTransforms(Map<String, TransformDetail> transformations,
                               List<TransformInfo> transformInfos,
                               Map<String, List<String>> connections) throws Exception {
      for (TransformInfo transformInfo : transformInfos) {
        String transformId = transformInfo.getTransformId();
        Transform transform = pluginContext.newPluginInstance(transformId);
        BatchRuntimeContext transformContext = new SparkBatchRuntimeContext(pluginContext, metrics,
                                                                            logicalStartTime, runtimeArgs, transformId);
        LOG.debug("Transform Class : {}", transform.getClass().getName());
        transform.initialize(transformContext);
        transformations.put(transformId, new TransformDetail(
          transform, new DefaultStageMetrics(metrics, transformId), connections.get(transformId)));
      }
    }
  }
}
