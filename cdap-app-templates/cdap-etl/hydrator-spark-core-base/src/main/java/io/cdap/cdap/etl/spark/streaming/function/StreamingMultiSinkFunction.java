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

package io.cdap.cdap.etl.spark.streaming.function;

import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.SubmitterLifecycle;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.lineage.AccessType;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.ExternalDatasets;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.etl.spark.SparkPipelineRuntime;
import io.cdap.cdap.etl.spark.SparkSubmitterContext;
import io.cdap.cdap.etl.spark.batch.SparkBatchSinkContext;
import io.cdap.cdap.etl.spark.batch.SparkBatchSinkFactory;
import io.cdap.cdap.etl.spark.function.MultiSinkFunction;
import io.cdap.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Function used to write a batch of data to a batch sink for use with a JavaDStream.
 * note: not using foreachRDD(VoidFunction2) method, because spark 1.3 doesn't have VoidFunction2.
 */
public class StreamingMultiSinkFunction implements Function2<JavaRDD<RecordInfo<Object>>, Time, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingMultiSinkFunction.class);
  private final JavaSparkExecutionContext sec;
  private final PhaseSpec phaseSpec;
  private final Set<String> group;
  private final Set<String> sinkNames;
  private final Map<String, StageStatisticsCollector> collectors;

  public StreamingMultiSinkFunction(JavaSparkExecutionContext sec, PhaseSpec phaseSpec,
                                    Set<String> group, Set<String> sinkNames,
                                    Map<String, StageStatisticsCollector> collectors) {
    this.sec = sec;
    this.phaseSpec = phaseSpec;
    this.group = group;
    this.sinkNames = sinkNames;
    this.collectors = collectors;
  }

  @Override
  public Void call(JavaRDD<RecordInfo<Object>> data, Time batchTime) throws Exception {
    long logicalStartTime = batchTime.milliseconds();
    MacroEvaluator evaluator = new DefaultMacroEvaluator(new BasicArguments(sec),
                                                         logicalStartTime,
                                                         sec.getSecureStore(),
                                                         sec.getNamespace());
    PluginContext pluginContext = new SparkPipelinePluginContext(sec.getPluginContext(), sec.getMetrics(),
                                                                 phaseSpec.isStageLoggingEnabled(),
                                                                 phaseSpec.isProcessTimingEnabled());
    SparkBatchSinkFactory sinkFactory = new SparkBatchSinkFactory();
    PipelineRuntime pipelineRuntime = new SparkPipelineRuntime(sec, logicalStartTime);

    Map<String, SubmitterLifecycle<?>> stages = createStages(evaluator);

    // call prepareRun() on all the stages in the group
    // need to call it in an order that guarantees that inputs are called before outputs
    // this is because plugins can call getArguments().set() in the prepareRun() method,
    // which downstream stages should be able to read
    List<String> traversalOrder = new ArrayList(group.size());
    for (String stageName : phaseSpec.getPhase().getDag().getTopologicalOrder()) {
      if (group.contains(stageName)) {
        traversalOrder.add(stageName);
      }
    }
    for (String stageName : traversalOrder) {
      SubmitterLifecycle<?> plugin = stages.get(stageName);
      StageSpec stageSpec = phaseSpec.getPhase().getStage(stageName);
      try {
        prepareRun(pipelineRuntime, sinkFactory, stageSpec, plugin);
      } catch (Exception e) {
        LOG.error("Error preparing sink {} for the batch for time {}.", stageName, logicalStartTime, e);
        return null;
      }
    }

    // run the actual transforms and sinks in this group
    boolean ranSuccessfully = true;
    try {
      MultiSinkFunction multiSinkFunction = new MultiSinkFunction(sec, phaseSpec, group, collectors);
      Set<String> outputNames = sinkFactory.writeCombinedRDD(data.flatMapToPair(Compat.convert(multiSinkFunction)),
                                                             sec, sinkNames);
      sec.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          for (String outputName : outputNames) {
            ExternalDatasets.registerLineage(sec.getAdmin(), outputName, AccessType.WRITE,
                                             null, () -> context.getDataset(outputName));
          }
        }
      });
    } catch (Exception e) {
      LOG.error("Error writing to sinks {} for the batch for time {}.", sinkNames, logicalStartTime, e);
      ranSuccessfully = false;
    }

    // run onRunFinish() for each sink
    for (String stageName : traversalOrder) {
      SubmitterLifecycle<?> plugin = stages.get(stageName);
      StageSpec stageSpec = phaseSpec.getPhase().getStage(stageName);
      try {
        onRunFinish(pipelineRuntime, sinkFactory, stageSpec, plugin, ranSuccessfully);
      } catch (Exception e) {
        LOG.warn("Unable to execute onRunFinish for sink {}", stageName, e);
      }
    }
    return null;
  }

  private Map<String, SubmitterLifecycle<?>> createStages(MacroEvaluator evaluator) throws InstantiationException {
    PluginContext pluginContext = sec.getPluginContext();
    Map<String, SubmitterLifecycle<?>> stages = new HashMap<>();
    for (String stageName : group) {
      SubmitterLifecycle<?> plugin = pluginContext.newPluginInstance(stageName, evaluator);
      stages.put(stageName, plugin);
    }
    return stages;
  }

  private void prepareRun(PipelineRuntime pipelineRuntime, SparkBatchSinkFactory sinkFactory, StageSpec stageSpec,
                          SubmitterLifecycle<?> plugin) throws TransactionFailureException {
    sec.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        if (stageSpec.getPluginType().equals(BatchSink.PLUGIN_TYPE)) {
          SparkBatchSinkContext context =
            new SparkBatchSinkContext(sinkFactory, sec, datasetContext, pipelineRuntime, stageSpec);
          ((SubmitterLifecycle<BatchSinkContext>) plugin).prepareRun(context);
          return;
        }

        SparkSubmitterContext context = new SparkSubmitterContext(sec, pipelineRuntime, datasetContext, stageSpec);
        ((SubmitterLifecycle<StageSubmitterContext>) plugin).prepareRun(context);
      }
    });
  }

  private void onRunFinish(PipelineRuntime pipelineRuntime, SparkBatchSinkFactory sinkFactory, StageSpec stageSpec,
                           SubmitterLifecycle<?> plugin,
                           boolean succeeded) throws TransactionFailureException {
    sec.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        if (stageSpec.getPluginType().equals(BatchSink.PLUGIN_TYPE)) {
          SparkBatchSinkContext sinkContext =
            new SparkBatchSinkContext(sinkFactory, sec, datasetContext, pipelineRuntime, stageSpec);
          ((SubmitterLifecycle<BatchSinkContext>) plugin).onRunFinish(succeeded, sinkContext);
          return;
        }

        SparkSubmitterContext context = new SparkSubmitterContext(sec, pipelineRuntime, datasetContext, stageSpec);
        ((SubmitterLifecycle<StageSubmitterContext>) plugin).onRunFinish(succeeded, context);
      }
    });
  }
}
