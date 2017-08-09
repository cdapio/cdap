/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.lineage;

import co.cask.cdap.etl.api.FieldLevelLineage;
import co.cask.cdap.etl.api.TransformStep;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * A FieldLevelLineageStorageGraph is a graph of the field lineage for a pipeline run
 */
public final class FieldLevelLineageStoreGraph {
  private final ProgramRunId pipelineId;
  private final PipelinePhase pipeline;
  private final Map<String, FieldLevelLineage> stages;
  private final Map<String, String> stageToDataset;
  private final Map<FieldLevelLineageStoreNode, FieldLevelLineageStoreNode> nodeRetriever;
  private final Table<String, String, DatasetFieldNode> history; // Storage
  private final Map<FieldStepNode, TransformStep> stepInformation; // Storage
  private final ListMultimap<FieldLevelLineageStoreNode, FieldLevelLineageStoreNode> pastEdges; // Storage
  private final ListMultimap<FieldLevelLineageStoreNode, FieldLevelLineageStoreNode> futureEdges; // Storage

  public FieldLevelLineageStoreGraph(ProgramRunId pipelineId, PipelinePhase pipeline,
                                     Map<String, FieldLevelLineage> stages, Map<String, String> stageToDataset) {
    this.pipelineId = pipelineId;
    this.pipeline = pipeline;
    this.stages = stages;
    this.stageToDataset = stageToDataset;
    this.nodeRetriever = new HashMap<>();
    this.history = HashBasedTable.create();
    this.stepInformation = new HashMap<>();
    this.pastEdges = ArrayListMultimap.create();
    this.futureEdges = ArrayListMultimap.create();
    this.make();
  }

  private int len(String stage, String field) {
    return stages.get(stage).getLineage().get(field).size();
  }

  private DatasetFieldNode create(String stage, String field) {
    DatasetFieldNode node = new DatasetFieldNode(stageToDataset.get(stage), pipelineId, stage, field);
    if (nodeRetriever.containsKey(node)) {
      return (DatasetFieldNode) nodeRetriever.get(node);
    } else {
      nodeRetriever.put(node, node);
      history.put(stageToDataset.get(stage), field, node);
      return node;
    }
  }

  private FieldStepNode create(String stage, String field, int stepNumber) {
    FieldStepNode node = new FieldStepNode(pipelineId, stage, field, stepNumber);
    if (nodeRetriever.containsKey(node)) {
      return (FieldStepNode) nodeRetriever.get(node);
    } else {
      nodeRetriever.put(node, node);
      stepInformation.put(node, stages.get(stage).getSteps().get(stepNumber));
      return node;
    }
  }

  private void forwardLineage(FieldLevelLineageStoreNode prev, String stage, String field, int index) {
    if (--index >= 0) {
      FieldLevelLineage.BranchingTransformStepNode stepNode = stages.get(stage).getLineage().get(field).get(index);
      if (stepNode.continueForward()) {
        FieldStepNode node = create(stage, field, stepNode.getTransformStepNumber());
        futureEdges.put(prev, node);
        prev = node;
        forwardLineage(prev, stage, field, index);
      }
      for (Map.Entry<String, Integer> entry : stepNode.getImpactedBranches().entrySet()) {
        forwardLineage(prev, stage, entry.getKey(), entry.getValue());
      }
    } else {
      //noinspection ConstantConditions
      for (String nextStage : pipeline.getDag().getNodeOutputs(stage)) {
        if (stages.containsKey(nextStage)) {
          forwardLineage(prev, nextStage, field, len(nextStage, field));
        } else if (pipeline.getSinks().contains(nextStage)) {
          futureEdges.put(prev, create(nextStage, field));
        }
      }
    }
  }

  private void backwardLineage(FieldLevelLineageStoreNode prev, String stage, String field, int index, int size) {
    if (++index < size) {
      FieldLevelLineage.BranchingTransformStepNode stepNode = stages.get(stage).getLineage().get(field).get(index);
      if (stepNode.continueBackward()) {
        FieldStepNode node = create(stage, field, stepNode.getTransformStepNumber());
        pastEdges.put(prev, node);
        prev = node;
        backwardLineage(prev, stage, field, index, size);
      }
      for (Map.Entry<String, Integer> entry : stepNode.getImpactingBranches().entrySet()) {
        backwardLineage(prev, stage, entry.getKey(), entry.getValue(), len(stage, entry.getKey()));
      }
    } else {
      //noinspection ConstantConditions
      for (String nextStage : pipeline.getDag().getNodeInputs(stage)) {
        if (stages.containsKey(nextStage)) {
          backwardLineage(prev, nextStage, field, -1, len(nextStage, field));
        } else if (pipeline.getSources().contains(nextStage)) {
          pastEdges.put(prev, create(nextStage, field));
        }
      }
    }
  }

  private void make() {
    if (pipeline.getDag() == null) {
      throw new IllegalArgumentException("Pipeline must be a DAG in order to generate lineage");
    }
    for (String dataset : pipeline.getSources()) {
      //noinspection ConstantConditions
      for (String field : pipeline.getStage(dataset).getOutputs()) {
        forwardLineage(create(dataset, field), dataset, field, 0);
      }
    }
    for (String dataset : pipeline.getSinks()) {
      //noinspection ConstantConditions
      for (String field : pipeline.getStage(dataset).getInputs()) {
        backwardLineage(create(dataset, field), dataset, field, 0, 0);
      }
    }
  }
}
