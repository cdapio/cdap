/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.spec;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.ErrorTransform;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.condition.Condition;
import io.cdap.cdap.etl.common.DefaultPipelineConfigurer;
import io.cdap.cdap.etl.common.DefaultStageConfigurer;
import io.cdap.cdap.etl.common.Schemas;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Keeps track of schemas for various stages of a pipeline and propagates them as they are updated.
 *
 * The {@link DefaultPipelineConfigurer DefaultPipelineConfigurers} keep state about the input, output,
 * and error schemas for each stage. As they are used to configure stages in the pipeline,
 * the output schema set by one stage must be propagated to the configurer for another stage.
 * This class performs that propagation.
 */
public class SchemaPropagator {
  private final Map<String, DefaultPipelineConfigurer> pluginConfigurers;
  private final Function<String, Set<String>> stageOutputsProvider;
  private final Function<String, String> stageTypeProvider;

  public SchemaPropagator(Map<String, DefaultPipelineConfigurer> pluginConfigurers,
                          Function<String, Set<String>> stageOutputsProvider,
                          Function<String, String> stageTypeProvider) {
    this.pluginConfigurers = pluginConfigurers;
    this.stageOutputsProvider = stageOutputsProvider;
    this.stageTypeProvider = stageTypeProvider;
  }

  /**
   * Propagate the output schema set for this stage as the input schema for all of its outputs.
   *
   * @param stageSpec the specification for the stage that was just configured
   */
  public void propagateSchema(StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    Set<String> nextStages = stageOutputsProvider.apply(stageName);

    for (String nextStageName : nextStages) {
      String nextStageType = stageTypeProvider.apply(nextStageName);
      Schema nextStageInputSchema = getNextStageInputSchema(stageSpec, nextStageName, nextStageType);

      DefaultStageConfigurer outputStageConfigurer = pluginConfigurers.get(nextStageName).getStageConfigurer();

      // Do not allow more than one input schema for stages other than Joiner and Action
      if (!BatchJoiner.PLUGIN_TYPE.equals(nextStageType)
        && !Action.PLUGIN_TYPE.equals(nextStageType)
        && !Condition.PLUGIN_TYPE.equals(nextStageType)
        && !hasSameSchema(outputStageConfigurer.getInputSchemas(), nextStageInputSchema)) {
        throw new IllegalArgumentException("Two different input schema were set for the stage " + nextStageName);
      }

      outputStageConfigurer.addInputSchema(stageName, nextStageInputSchema);
      outputStageConfigurer.addInputStage(stageName);
    }
  }

  /**
   * Get the input schemas for the next stage that come from the current stage. Unless the current stage is a
   * connector source, the map will always contain just a single entry.
   */
  @Nullable
  private Schema getNextStageInputSchema(StageSpec currentStageSpec, String nextStageName, String nextStageType) {
    if (ErrorTransform.PLUGIN_TYPE.equals(nextStageType)) {
      // if the output stage is an error transform, it takes the error schema of this stage as its input.
      return currentStageSpec.getErrorSchema();
    } else if (SplitterTransform.PLUGIN_TYPE.equals(currentStageSpec.getPlugin().getType())) {
      // if the current stage is a splitter transform, it takes the output schema of the port it is connected to
      // all other plugin types that the output schema of this stage as its input.
      StageSpec.Port portSpec = currentStageSpec.getOutputPorts().get(nextStageName);
      // port can be null if no output ports were specified at configure time
      // this can happen if the ports are dependent on the data received by the plugin
      if (portSpec == null) {
        return null;
      } else if (portSpec.getPort() == null) {
        // null if a splitter was connected to another stage without a port specified.
        // Should not happen since it should have been validated earlier, but check here just in case
        throw new IllegalArgumentException(String.format("Must specify a port when connecting Splitter '%s' to '%s'",
                                                         currentStageSpec.getName(), nextStageName));
      } else {
        return portSpec.getSchema();
      }
    } else {
      return currentStageSpec.getOutputSchema();
    }
  }

  private boolean hasSameSchema(Map<String, Schema> inputSchemas, @Nullable Schema inputSchema) {
    if (!inputSchemas.isEmpty()) {
      Schema s = inputSchemas.values().iterator().next();
      if (s == null && inputSchema == null) {
        return true;
      } else if (s == null) {
        return false;
      } else if (inputSchema == null) {
        return false;
      }

      return Schemas.equalsIgnoringRecordName(s, inputSchema);
    }
    return true;
  }

}
