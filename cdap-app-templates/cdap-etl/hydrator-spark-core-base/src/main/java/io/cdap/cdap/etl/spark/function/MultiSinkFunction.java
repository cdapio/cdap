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

package io.cdap.cdap.etl.spark.function;

import com.google.common.collect.Sets;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.ErrorTransform;
import io.cdap.cdap.etl.batch.PipelinePluginInstantiator;
import io.cdap.cdap.etl.batch.connector.SingleConnectorFactory;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultEmitter;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.RecordType;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.exec.PipeTransformExecutor;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkTransformExecutorFactory;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Executes transforms leading into a grouped sink.
 * With a pipeline like:
 *
 *      |--> t1 --> k1
 * s1 --|
 *      |--> k2
 *           ^
 *     s2 ---|
 *
 * the group will include stages t1, k1, and k2:
 *
 *  s1 --|
 *       |--> group1
 *  s2 --|
 *
 * note that although s1 and s2 are both inputs to the group, the records for s2 should only be sent to
 * k2 and not to t1. Thus, every input record must be tagged with the stage that it came from, so that they can
 * be sent to the right underlying stages. This is why the input is a RecordInfo.
 *
 * This function is meant to be executed right before saving the Spark collection using a Multi OutputFormat that
 * delegates to underlying output formats.
 */
public class MultiSinkFunction implements PairFlatMapFunc<RecordInfo<Object>, String, KeyValue<Object, Object>> {
  private final PipelineRuntime pipelineRuntime;
  private final PhaseSpec phaseSpec;
  private final Set<String> group;
  private final Map<String, DataTracer> dataTracers;
  private final Map<String, StageStatisticsCollector> collectors;
  private transient DefaultEmitter<Tuple2<String, KeyValue<Object, Object>>> emitter;
  private transient SparkTransformExecutorFactory executorFactory;
  private transient Map<InputInfo, Set<String>> inputConnections;
  private transient Map<String, PipeTransformExecutor<Object>> branchExecutors;

  public MultiSinkFunction(JavaSparkExecutionContext sec, PhaseSpec phaseSpec, Set<String> group,
                           Map<String, StageStatisticsCollector> collectors) {
    this.pipelineRuntime = new PipelineRuntime(
      sec.getNamespace(), sec.getApplicationSpecification().getName(), sec.getLogicalStartTime(),
      new BasicArguments(sec), sec.getMetrics(), sec.getPluginContext(), sec.getServiceDiscoverer(),
      sec.getSecureStore(), null, null);
    // create a copy because BatchPhaseSpec contains things that are not serializable while PhaseSpec does not
    this.phaseSpec = new PhaseSpec(phaseSpec.getPhaseName(), phaseSpec.getPhase(), phaseSpec.getConnectorDatasets(),
                                   phaseSpec.isStageLoggingEnabled(), phaseSpec.isProcessTimingEnabled());
    this.group = group;
    this.collectors = collectors;
    this.dataTracers = new HashMap<>();
    for (String stage : group) {
      dataTracers.put(stage, sec.getDataTracer(stage));
    }
  }

  @Override
  public Iterable<Tuple2<String, KeyValue<Object, Object>>> call(RecordInfo<Object> input) throws Exception {
    if (branchExecutors == null) {
      // branch executors must be created lazily here instead of passed into the constructor to ensure that
      // they are not serialized in the function. This ensures that macros are evaluated each run instead of just for
      // the first run and then serialized.
      initializeBranchExecutors();
    }

    /*
       Input records are a union of RecordInfo<Object> from all possible inputs to the group.
       For example, suppose the pipeline looks like:

                              |port A --> agg --> k1
            s1 --> splitter --|
                      |       |port B --> k2
                      |
                      |--> error collector --> k3

       In this scenario, k2, error collector, and k3 are grouped together.
       However, the input will contain all records output by the splitter.
       More specifically, it contains outputs for portA, outputs for portB, and errors.
       Records from portB need to be sent to the k2 branch, errors need to be sent to the error collector branch,
       and portA records need to be dropped.
     */
    InputInfo inputInfo = new InputInfo(input.getFromStage(), input.getType(), input.getFromPort());
    Object record = input.getValue();
    emitter.reset();

    /*
        inputConnections contains a map from input source to the branch that should receive it.
        With the example pipeline above, it will look like:
          { stageName: splitter, port: B, type: output } -> [k2]
          { stageName: splitter, type: error } -> [error collector]
     */
    Set<String> groupSources = inputConnections.getOrDefault(inputInfo, Collections.emptySet());
    for (String groupSource : groupSources) {
      branchExecutors.get(groupSource).runOneIteration(record);
    }

    return emitter.getEntries();
  }

  private void initializeBranchExecutors() {
    emitter = new DefaultEmitter<>();
    PipelinePluginInstantiator pluginInstantiator =
      new PipelinePluginInstantiator(pipelineRuntime.getPluginContext(), pipelineRuntime.getMetrics(),
                                     phaseSpec, new SingleConnectorFactory());
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(
      pipelineRuntime.getArguments(), pipelineRuntime.getLogicalStartTime(), pipelineRuntime.getSecureStore(),
      pipelineRuntime.getServiceDiscoverer(), pipelineRuntime.getNamespace());
    executorFactory = new SparkTransformExecutorFactory(pluginInstantiator, macroEvaluator, null,
                                                        collectors, dataTracers, pipelineRuntime, emitter);

    /*
       If the dag is:

            |--> t1 --> k1
       s1 --|
            |--> k2
                 ^
           s2 ---|

       the group is t1, k1, and k2.
     */
    PipelinePhase pipelinePhase = phaseSpec.getPhase();
    branchExecutors = new HashMap<>();
    inputConnections = new HashMap<>();
    for (String groupSource : group) {
      // start by finding the "sources" of the group (t1 and k2 in the example above).
      // group "sources" are stages in the group that don't have an input from another stage in the group.
      if (Sets.difference(pipelinePhase.getStageInputs(groupSource), group).isEmpty()) {
        continue;
      }

      // get the branch by taking a subset of the pipeline starting from the "source".
      // with the example above, the two branches are t1 -> k1, and k2.
      PipelinePhase branch;
      if (pipelinePhase.getSinks().contains(groupSource)) {
        // pipelinePhase.subsetFrom() throws an exception if the new "source" is also a sink,
        // since a Dag cannot be a single node. so build it manually.
        branch = PipelinePhase.builder(pipelinePhase.getPluginTypes())
          .addStage(pipelinePhase.getStage(groupSource))
          .build();
      } else {
        branch = pipelinePhase.subsetFrom(Collections.singleton(groupSource));
      }
      try {
        branchExecutors.put(groupSource, executorFactory.create(branch));
      } catch (Exception e) {
        throw new IllegalStateException(
          String.format("Unable to get subset of pipeline starting from stage %s. " +
                          "This indicates a planning error. Please report this bug and turn off stage " +
                          "consolidation by setting %s to false in the runtime arguments.",
                        groupSource, Constants.CONSOLIDATE_STAGES), e);
      }

      /*
          create a mapping from possible inputs to "group sources". This will help identify which incoming
          records should be sent to which branch executor.

          for example, the pipeline may look like:

                           |port a --> k1
             s --> split --|
                           |port b --> k2

          In this scenario, k1, and k2, are all in the same group, so the map contains:

            { stageName: split, port: a, type: output } -> [k1]
            { stageName: split, port: b, type: output } -> [k2]

          A slightly more complicated example:

                               |--> k1
            s1 --> transform --|
                      |        |--> k2
                      |
                      |--> error collector --> k3

          In this scenario, k1, k2, k3, and error collector are in the same group, so the map contains:

            { stageName: transform, type: output } -> [k1, k2]
            { stageName: transform, type: error } -> [k3]
       */
      String groupSourceType = pipelinePhase.getStage(groupSource).getPluginType();
      RecordType recordType = ErrorTransform.PLUGIN_TYPE.equals(groupSourceType) ? RecordType.ERROR : RecordType.OUTPUT;
      for (String inputStage : pipelinePhase.getStageInputs(groupSource)) {
        Map<String, StageSpec.Port> ports = pipelinePhase.getStage(inputStage).getOutputPorts();
        String port = ports.get(groupSource).getPort();
        InputInfo inputInfo = new InputInfo(inputStage, recordType, port);
        Set<String> groupSources = inputConnections.computeIfAbsent(inputInfo, key -> new HashSet<>());
        groupSources.add(groupSource);
      }
    }
  }

  /**
   * Information about where input is coming from.
   */
  private static class InputInfo {
    private final String stageName;
    private final RecordType type;
    private final String port;

    private InputInfo(String stageName, RecordType type, @Nullable String port) {
      this.stageName = stageName;
      this.port = port;
      this.type = type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof InputInfo)) {
        return false;
      }
      InputInfo that = (InputInfo) o;
      return Objects.equals(stageName, that.stageName) &&
        type == that.type &&
        Objects.equals(port, that.port);
    }

    @Override
    public int hashCode() {
      return Objects.hash(stageName, type, port);
    }
  }
}
