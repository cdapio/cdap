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
 */

package io.cdap.cdap.etl.lineage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.common.FieldOperationTypeAdapter;
import io.cdap.cdap.etl.proto.v2.spec.PipelineSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Field lineage processor to validate the stage operations and convert the pipeline level operations to platform
 * level operations
 */
public class FieldLineageProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(FieldLineageProcessor.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(FieldOperation.class, new FieldOperationTypeAdapter()).create();

  private final PipelineSpec pipelineSpec;

  public FieldLineageProcessor(PipelineSpec pipelineSpec) {
    this.pipelineSpec = pipelineSpec;
  }

  public Set<Operation> validateAndConvert(Map<String, List<FieldOperation>> allStageOperations) {
    Map<String, List<FieldOperation>> allOperations = new HashMap<>(allStageOperations);
    // Set of stages for which no implicit merge operation is required even if
    // stage has multiple inputs, for example join stages
    Set<String> noMergeRequiredStages = new HashSet<>();
    for (StageSpec stageSpec : pipelineSpec.getStages()) {
      if (BatchJoiner.PLUGIN_TYPE.equals(stageSpec.getPlugin().getType())) {
        noMergeRequiredStages.add(stageSpec.getName());
      }
    }

    // validate the stage operations
    Map<String, InvalidFieldOperations> stageInvalids = new HashMap<>();
    Map<String, Map<String, List<String>>> stageRedundants = new HashMap<>();
    for (StageSpec stageSpec : pipelineSpec.getStages()) {

      Map<String, Schema> inputSchemas = stageSpec.getInputSchemas();
      // If current stage is of type JOIN add fields as inputstageName.fieldName
      List<String> stageInputs = new ArrayList<>();
      List<String> stageOutputs = new ArrayList<>();
      if (BatchJoiner.PLUGIN_TYPE.equals(stageSpec.getPlugin().getType())) {
        for (Map.Entry<String, Schema> entry : inputSchemas.entrySet()) {
          if (entry.getValue().getFields() != null) {
            stageInputs.addAll(entry.getValue().getFields()
                                 .stream().map(field -> entry.getKey() + "." + field.getName())
                                 .collect(Collectors.toList()));
          }
        }
      } else {
        for (Map.Entry<String, Schema> entry : inputSchemas.entrySet()) {
          if (entry.getValue().getFields() != null) {
            stageInputs.addAll(entry.getValue().getFields().stream().map(Schema.Field::getName)
                                 .collect(Collectors.toList()));
          }
        }
      }

      Schema outputSchema = stageSpec.getOutputSchema();
      if (outputSchema != null && outputSchema.getFields() != null) {
        stageOutputs.addAll(outputSchema.getFields().stream().map(Schema.Field::getName)
                              .collect(Collectors.toList()));
      }

      String stageName = stageSpec.getName();

      List<FieldOperation> fieldOperations = allOperations.computeIfAbsent(stageName, stage -> Collections.emptyList());
      StageOperationsValidator.Builder builder = new StageOperationsValidator.Builder(fieldOperations);
      builder.addStageInputs(stageInputs);
      builder.addStageOutputs(stageOutputs);
      StageOperationsValidator stageOperationsValidator = builder.build();
      stageOperationsValidator.validate();
      LOG.trace("Stage Name: {}", stageName);
      LOG.trace("Stage Operations {}", GSON.toJson(fieldOperations));
      LOG.trace("Stage inputs: {}", stageInputs);
      LOG.trace("Stage outputs: {}", stageOutputs);
      InvalidFieldOperations invalidFieldOperations = stageOperationsValidator.getStageInvalids();
      if (invalidFieldOperations != null) {
        stageInvalids.put(stageName, invalidFieldOperations);
      }

      if (!stageOperationsValidator.getRedundantOutputs().isEmpty()) {
        stageRedundants.put(stageName, stageOperationsValidator.getRedundantOutputs());
      }
    }

    if (!stageRedundants.isEmpty()) {
      LOG.debug("The pipeline has redundant operations {} and they will be ignored", stageRedundants);
    }

    if (!stageInvalids.isEmpty()) {
      // Do not throw but just log the exception message for validation failure
      // Once most of the plugins are updated to write lineage exception can be thrown
      LOG.debug(new InvalidLineageException(stageInvalids).getMessage());
    }

    LineageOperationsProcessor processor = new LineageOperationsProcessor(pipelineSpec.getConnections(),
                                                                          allOperations, noMergeRequiredStages);

    return processor.process();
  }
}
