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

package co.cask.cdap.etl.spec;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.workflow.NodeValue;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.SplitterTransform;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.condition.Condition;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultPipelineConfigurer;
import co.cask.cdap.etl.common.DefaultStageConfigurer;
import co.cask.cdap.etl.proto.v2.spec.StageSpec;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Keeps track of schemas for various stages of a pipeline and propagates them as they are updated.
 *
 * The DefaultPipelineConfigurer's keep state about the input, output, and error schemas for each stage.
 * As they are used to configure stages in the pipeline, the output schema set by one stage must be propagated to
 * the configurer for another stage. This class performs that propagation.
 *
 * Also propagates schema across pipeline phases using a WorkflowToken.
 * Propagating schema across phases means propagating it across connectors.
 * Whenever there is a connector sink in one phase, there will be a corresponding connector source in another
 * phase. The input schemas going into the connector sink must be set as the input schemas for any stage
 * connected to the connector source.
 *
 * Consider the following example pipeline:
 *
 *
 *   source1 --> aggregator --|
 *                            |--> joiner --> sink
 *   source2 -----------------|
 *
 *
 * This pipeline will get broken down into the following three phases:
 *
 *   source --> aggregator --> joiner.connector
 *                                                      joiner.connector --> joiner --> sink
 *   source2 --> joiner.connector
 *
 * The joiner.connector sink has two input schemas, one for 'aggregator' and one for 'source2'.
 * These input schemas need to be passed onto the third phase, where the joiner.connector source will read them
 * and set them as the input schemas to the 'joiner' stage.
 *
 * To accomplish this, connector input schemas are stored in the workflow token, each input schema with its own key.
 * In the example above, when the schema from 'aggregator' is propagated for the 'joiner.connector' sink, it is written
 * to the workflow token with key '__cs|joiner|aggregator'. Similarly, when schema from 'source2' is
 * propagated for the 'joiner' sink, it is written to the workflow token
 * with key '__cs|joiner|source2'.
 *
 * When schema is propagated for the 'joiner.connector' source, workflow properties that start with
 * '__cs|joiner|' are fetched, and the full set of input schemas is extracted.
 *
 * In these examples, '|' is used for clarity, but the actual separator is unicode 1.
 */
public class SchemaPropagator {
  private static final String TOKEN_PREFIX = "__cs";
  private static final String TOKEN_SEPARATOR = "\u0001";
  private final Map<String, DefaultPipelineConfigurer> pluginConfigurers;
  private final Function<String, Set<String>> stageOutputsProvider;
  private final Function<String, String> stageTypeProvider;
  private final WorkflowToken workflowToken;

  public SchemaPropagator(Map<String, DefaultPipelineConfigurer> pluginConfigurers,
                          Function<String, Set<String>> stageOutputsProvider,
                          Function<String, String> stageTypeProvider,
                          @Nullable WorkflowToken workflowToken) {
    this.pluginConfigurers = pluginConfigurers;
    this.stageOutputsProvider = stageOutputsProvider;
    this.stageTypeProvider = stageTypeProvider;
    this.workflowToken = workflowToken;
  }

  /**
   * Propagate the output schema set for this stage as the input schema for all of its outputs.
   *
   * @param stageSpec the specification for the stage that was just configured
   */
  public void propagateSchema(StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    String stageType = stageSpec.getPluginType();
    Set<String> nextStages = stageOutputsProvider.apply(stageName);

    // if we're propagating schema for a connector sink, write the schema to the workflow token.
    if (Constants.Connector.PLUGIN_TYPE.equals(stageType) && nextStages.isEmpty()) {
      writeInputSchemas(stageSpec.getPlugin().getProperties().get(Constants.Connector.ORIGINAL_NAME),
                        stageSpec.getInputSchemas());
      return;
    }

    for (String nextStageName : nextStages) {
      String nextStageType = stageTypeProvider.apply(nextStageName);
      Map<String, Schema> nextStageInputSchemas = getNextStageInputSchema(stageSpec, nextStageName, nextStageType);

      DefaultStageConfigurer outputStageConfigurer = pluginConfigurers.get(nextStageName).getStageConfigurer();

      // Do not allow more than one input schema for stages other than Joiner and Action
      for (Schema nextStageInputSchema : nextStageInputSchemas.values()) {
        if (!BatchJoiner.PLUGIN_TYPE.equals(nextStageType)
          && !Action.PLUGIN_TYPE.equals(nextStageType)
          && !Condition.PLUGIN_TYPE.equals(nextStageType)
          && !hasSameSchema(outputStageConfigurer.getInputSchemas(), nextStageInputSchema)) {
          throw new IllegalArgumentException("Two different input schema were set for the stage " + nextStageName);
        }
      }

      for (Map.Entry<String, Schema> entry : nextStageInputSchemas.entrySet()) {
        outputStageConfigurer.addInputSchema(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Get the input schemas for the next stage that come from the current stage. Unless the current stage is a
   * connector source, the map will always contain just a single entry.
   */
  private Map<String, Schema> getNextStageInputSchema(StageSpec currentStageSpec, String nextStageName,
                                                      String nextStageType) {
    if (isConnectorSource(currentStageSpec)) {
      // read input schemas from the workflow token
      if (workflowToken == null) {
        // should never happen. Indicates a bug in the code
        throw new IllegalStateException("Error while propagating schema across pipeline phases. "
                                          + "Could not find workflow token.");
      }
      return readInputSchemas(currentStageSpec.getPlugin().getProperties().get(Constants.Connector.ORIGINAL_NAME));
    } else if (ErrorTransform.PLUGIN_TYPE.equals(nextStageType)) {
      // if the output stage is an error transform, it takes the error schema of this stage as its input.
      return Collections.singletonMap(currentStageSpec.getName(), currentStageSpec.getErrorSchema());
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
        return Collections.singletonMap(currentStageSpec.getName(), portSpec.getSchema());
      }
    } else {
      return Collections.singletonMap(currentStageSpec.getName(), currentStageSpec.getOutputSchema());
    }
  }

  private boolean isConnectorSource(StageSpec stageSpec) {
    return Constants.Connector.PLUGIN_TYPE.equals(stageSpec.getPluginType()) &&
      Constants.Connector.SOURCE_TYPE.equals(stageSpec.getPlugin().getProperties().get(Constants.Connector.TYPE));
  }

  private void writeInputSchemas(String stageName, Map<String, Schema> schemas) {
    for (Map.Entry<String, Schema> entry : schemas.entrySet()) {
      String key = String.join(TOKEN_SEPARATOR, TOKEN_PREFIX, stageName, entry.getKey());
      if (entry.getValue() != null && workflowToken != null) {
        workflowToken.put(key, entry.getValue().toString());
      }
    }
  }

  private Map<String, Schema> readInputSchemas(String stageName) {
    Map<String, Schema> result = new HashMap<>();
    for (Map.Entry<String, List<NodeValue>> entry : workflowToken.getAll().entrySet()) {
      String key = entry.getKey();
      if (!key.startsWith(TOKEN_PREFIX + TOKEN_SEPARATOR + stageName + TOKEN_SEPARATOR)) {
        continue;
      }
      List<NodeValue> schemaValues = entry.getValue();
      if (schemaValues.size() < 1) {
        throw new IllegalStateException(
          String.format("Error while propagating schema across pipeline phases. "
                          + "Could not find input schema for stage '%s'.", stageName));
      }
      int sepIndex = key.lastIndexOf(TOKEN_SEPARATOR);
      String inputStageName = key.substring(sepIndex);

      String schemaStr = schemaValues.iterator().next().getValue().toString();
      try {
        result.put(inputStageName, Schema.parseJson(schemaStr));
      } catch (IOException e) {
        // should never happen
        throw new IllegalStateException(String.format("Unable to parse input schema for stage '%s'.", stageName));
      }
    }
    return result;
  }

  private boolean hasSameSchema(Map<String, Schema> inputSchemas, Schema inputSchema) {
    if (!inputSchemas.isEmpty()) {
      return Objects.equals(inputSchemas.values().iterator().next(), inputSchema);
    }
    return true;
  }

}
