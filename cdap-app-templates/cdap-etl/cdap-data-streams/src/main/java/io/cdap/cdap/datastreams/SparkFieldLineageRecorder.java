/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.datastreams;

import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.plugin.PipelinePluginContext;
import io.cdap.cdap.etl.lineage.FieldLineageProcessor;
import io.cdap.cdap.etl.proto.v2.spec.PipelineSpec;
import io.cdap.cdap.etl.spark.SparkPipelineRuntime;
import io.cdap.cdap.etl.spark.streaming.SparkStreamingPreparer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Field lineage recorder for streaming pipeline
 */
public class SparkFieldLineageRecorder {
  private final JavaSparkExecutionContext sec;
  private final PipelinePhase pipelinePhase;
  private final PipelineSpec pipelineSpec;
  private Map<String, List<FieldOperation>> operations;

  public SparkFieldLineageRecorder(JavaSparkExecutionContext sec,
                                   PipelinePhase pipelinePhase, PipelineSpec pipelineSpec) {
    this.sec = sec;
    this.pipelinePhase = pipelinePhase;
    this.pipelineSpec = pipelineSpec;
    this.operations = new HashMap<>();
  }

  public void record() throws Exception {
    if (operations.isEmpty()) {
      generateOperations();
    }

    FieldLineageProcessor processor = new FieldLineageProcessor(pipelineSpec);
    Set<Operation> processedOperations = processor.validateAndConvert(operations);
    if (!processedOperations.isEmpty()) {
      sec.record(processedOperations);
      sec.flushLineage();
    }
  }

  private void generateOperations() throws Exception {
    PipelinePluginContext pluginContext = new PipelinePluginContext(sec.getPluginContext(), sec.getMetrics(),
                                                                    pipelineSpec.isStageLoggingEnabled(),
                                                                    pipelineSpec.isProcessTimingEnabled());
    PipelineRuntime pipelineRuntime = new SparkPipelineRuntime(sec);
    MacroEvaluator evaluator = new DefaultMacroEvaluator(pipelineRuntime.getArguments(),
                                                         sec.getLogicalStartTime(), sec,
                                                         sec.getNamespace());
    SparkStreamingPreparer preparer = new SparkStreamingPreparer(pluginContext, sec.getMetrics(), evaluator,
                                                                 pipelineRuntime, sec);
    preparer.prepare(new PhaseSpec(DataStreamsSparkLauncher.NAME, pipelinePhase, Collections.emptyMap(),
                                   pipelineSpec.isStageLoggingEnabled(), pipelineSpec.isProcessTimingEnabled()));
    operations = preparer.getFieldOperations();
  }
}
