/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.datapipeline;

import co.cask.cdap.api.lineage.field.InputField;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.api.lineage.field.FieldReadOperation;
import co.cask.cdap.etl.api.lineage.field.FieldTransformOperation;
import co.cask.cdap.etl.api.lineage.field.FieldWriteOperation;
import co.cask.cdap.etl.planner.Dag;
import co.cask.cdap.etl.proto.Connection;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Class responsible for processing the field lineage operations recorded by plugins and
 * converting it into the form the platform expects. This includes prefixing operation names
 * to make sure they are unique across all the plugins, adding implicit merge operations when
 * stage has more than one input etc.
 */
public class LineageOperationsProcessor {
  private static final String SEPARATOR = ".";

  private final List<String> topologicalOrder;
  private final Dag stageDag;
  // Map of stage to list of operations recorded by that stage
  private final Map<String, List<FieldOperation>> stageOperations;
  // Map of stage name to another map which contains the output field names to the corresponding origin
  private final Map<String, Map<String, String>> stageOutputsWithOrigins;
  // Set of stages which requires no implicit merge operation for example stages of type join
  private final Set<String> noMergeRequiredStages;
  private Map<String, Operation> processedOperations;

  public LineageOperationsProcessor(Set<Connection> stageConnections,
                                    Map<String, List<FieldOperation>> stageOperations,
                                    Set<String> noMergeRequiredStages) {
    this.stageDag = new Dag(stageConnections);
    this.topologicalOrder = stageDag.getTopologicalOrder();
    this.stageOperations = stageOperations;
    this.stageOutputsWithOrigins = new HashMap<>();
    for (String stage : topologicalOrder) {
      stageOutputsWithOrigins.put(stage, new LinkedHashMap<>());
    }
    this.noMergeRequiredStages = noMergeRequiredStages;
  }

  /**
   * @return the operations which will be submitted to the platform
   */
  public Set<Operation> process() {
    if (processedOperations == null) {
      processedOperations = computeProcessedOperations();
    }
    return new HashSet<>(processedOperations.values());
  }

  private Map<String, Operation> computeProcessedOperations() {
    Map<String, Operation> processedOperations = new HashMap<>();
    for (String stageName : topologicalOrder) {
      Set<String> stageInputs = stageDag.getNodeInputs(stageName);
      if (stageInputs.size() > 1 && !noMergeRequiredStages.contains(stageName)) {
        addMergeOperation(stageInputs, processedOperations);
      }
      List<FieldOperation> fieldOperations = stageOperations.get(stageName);
      for (FieldOperation fieldOperation : fieldOperations) {
        Operation newOperation = null;
        String newOperationName =  prefixedOperationName(stageName, fieldOperation.getName());
        Set<String> currentOperationOutputs = new LinkedHashSet<>();
        switch (fieldOperation.getType()) {
          case READ:
            FieldReadOperation read = (FieldReadOperation) fieldOperation;
            newOperation = new ReadOperation(newOperationName, read.getDescription(),
                                              read.getSource(), read.getOutputFields());
            currentOperationOutputs.addAll(read.getOutputFields());
            break;
          case TRANSFORM:
            FieldTransformOperation transform = (FieldTransformOperation) fieldOperation;
            List<InputField> inputFields = createInputFields(transform.getInputFields(), stageName,
                                                             processedOperations);
            newOperation = new TransformOperation(newOperationName, transform.getDescription(), inputFields,
                                                  transform.getOutputFields());
            currentOperationOutputs.addAll(transform.getOutputFields());
            break;
          case WRITE:
            FieldWriteOperation write = (FieldWriteOperation) fieldOperation;
            inputFields = createInputFields(write.getInputFields(), stageName, processedOperations);
            newOperation = new WriteOperation(newOperationName, write.getDescription(), write.getSink(), inputFields);
            break;
        }
        for (String currentOperationOutput : currentOperationOutputs) {
          // For all fields outputted by the current operation assign the operation name as origin
          // If the field appears in the output again for some other operation belonging to the same stage,
          // its origin will get updated to the new operation
          stageOutputsWithOrigins.get(stageName).put(currentOperationOutput, newOperation.getName());
        }
        processedOperations.put(newOperation.getName(), newOperation);
      }
    }
    return processedOperations;
  }

  private void addMergeOperation(Set<String> stageInputs,
                                 Map<String, Operation> processedOperations) {
    Set<String> sortedInputs = new TreeSet<>(stageInputs);
    String mergeOperationName = prefixedOperationName(Joiner.on(SEPARATOR).join(sortedInputs), "merge");
    String mergeDescription = "Merged stages: " + Joiner.on(",").join(sortedInputs);
    if (processedOperations.containsKey(mergeOperationName)) {
      // it is possible that same stages act as an input to multiple stages.
      // we should still only add single merge operation for them
      return;
    }
    List<InputField> inputFields = new ArrayList<>();
    for (String inputStage : sortedInputs) {
      List<String> parentStages = findParentStages(inputStage);
      for (String parentStage : parentStages) {
        Map<String, String> fieldOrigins = stageOutputsWithOrigins.get(parentStage);
        for (Map.Entry<String, String> fieldOrigin : fieldOrigins.entrySet()) {
          inputFields.add(InputField.of(fieldOrigin.getValue(), fieldOrigin.getKey()));
        }
      }
    }

    Set<String> outputs = new LinkedHashSet<>();
    for (InputField inputField : inputFields) {
      outputs.add(inputField.getName());
    }
    TransformOperation merge = new TransformOperation(mergeOperationName, mergeDescription, inputFields,
                                                      new ArrayList<>(outputs));
    processedOperations.put(merge.getName(), merge);
  }

  /**
   * Create {@link InputField}s from field names which acts as an input to the operation. Creating
   * InputField requires origin; the name of the operation which created the field. To figure out the
   * origin, we traverse in the reverse direction from the current stage until we reach source. While
   * traversing, for each stage we check if the field occurs in any of the output operations recorded
   * by that stage, if so we have found the origin.
   *
   * @param fields the List of input field names for an operation for which InputFields to be created
   * @param currentStage name of the stage which recorded the operation
   * @param processedOperations processed operations so far
   * @return List of InputFields
   */
  private List<InputField> createInputFields(List<String> fields, String currentStage,
                                             Map<String, Operation> processedOperations) {
    // We need to return InputFields in the same order as fields.
    // Keep them in map so we can iterate later on received fields to return InputFields.

    Map<String, InputField> inputFields = new HashMap<>();
    List<String> parents = findParentStages(currentStage);

    List<String> fieldsForJoin = new ArrayList<>();
    for (String field : fields) {
      if (noMergeRequiredStages.contains(currentStage)) {
        // Current stage is of type JOIN.
        // JOIN creates operation with input fields in format <stagename.fieldname>
        // if the field is directly read from the schema of the one of the input stage.
        // However it is also possible to have operation with input not tagged with the stage name, for example
        // in case if rename is done on the join keys. In this case Join operation will read fields of the form
        // <stagename.fieldname> however rename operations followed by it will simply read the output of the
        // Join which wont be tagged with stagename.

        //TODO CDAP-13298 Field names and stage names can have '.' in them.
        // Probably better to support new JOIN operation type which takes stage name and field name.
        Iterator<String> stageFieldPairIter = Splitter.on(".").omitEmptyStrings().trimResults().split(field).iterator();
        String stageName = stageFieldPairIter.next();
        if (stageFieldPairIter.hasNext() && stageOperations.keySet().contains(stageName)) {
          field = stageFieldPairIter.next();
          fieldsForJoin.add(field);
          List<String> parentStages = findParentStages(stageName);
          parents.addAll(0, parentStages);
        } else {
          // field belongs to one of the operations output from join stage
          fieldsForJoin.add(field);
        }
      }
      ListIterator<String> parentsIterator = parents.listIterator(parents.size());
      while (parentsIterator.hasPrevious()) {
        // Iterate in parents in the reverse order so that we find the most recently updated origin
        // first
        String stage = parentsIterator.previous();
        // check if the current field is output by any one of operation created by current stage
        String origin = stageOutputsWithOrigins.get(stage).get(field);
        if (origin != null) {
          inputFields.put(field, InputField.of(origin, field));
          break;
        }
      }
    }

    Set<String> stageInputs = stageDag.getNodeInputs(parents.get(0));
    if (stageInputs.size() > 1 && !noMergeRequiredStages.contains(parents.get(0))) {
      String mergeOperationName = mergeOperationName(stageInputs);
      Operation operation = processedOperations.get(mergeOperationName);
      List<String> outputs = ((TransformOperation) operation).getOutputs();
      for (String field : fields) {
        // Only add the InputFields corresponding to the remaining fields
        if (outputs.contains(field) && inputFields.get(field) == null) {
          inputFields.put(field, InputField.of(mergeOperationName, field));
        }
      }
    }

    List<String> fieldsToIterateOn = noMergeRequiredStages.contains(currentStage) ? fieldsForJoin : fields;
    List<InputField> result = new ArrayList<>();
    for (String field : fieldsToIterateOn) {
      if (inputFields.containsKey(field)) {
        result.add(inputFields.get(field));
      }
    }
    return result;
  }

  private List<String> findParentStages(String stageName) {
    List<String> parents = new ArrayList<>();
    String currentStage = stageName;
    while (true) {
      List<String> currentBranch = stageDag.getBranch(currentStage, new HashSet<>());
      parents.addAll(0, currentBranch);
      String headStage = parents.get(0);
      Set<String> nodeInputs = stageDag.getNodeInputs(headStage);
      if (nodeInputs.size() == 1) {
        // It is possible that the input node to the currentBranch has multiple outputs
        // Include the nodes on that branch as well.
        // For example:
        //               |--->n2--->n3
        //     n0-->n1-->|
        //               |--->n4--->n5
        //
        // If we want to find all parent stages of n3, stageDag.getBranch("n3") will only give us the branch
        // n2--->n3. However we still need to include n0-->n1 branch in the result for finding origins
        // since n1 has two outputs we know that its a different branch and need to be added
        String branchInputNode = nodeInputs.iterator().next();
        if (stageDag.getNodeOutputs(branchInputNode).size() > 1) {
          currentStage = branchInputNode;
          continue;
        }
      }
      break;
    }
    return parents;
  }

  private String mergeOperationName(Set<String> inputStages) {
    Set<String> sortedInputs = new TreeSet<>(inputStages);
    return prefixedOperationName(Joiner.on(SEPARATOR).join(sortedInputs), "merge");
  }

  private String prefixedOperationName(String stageName, String operationName) {
    return stageName + SEPARATOR + operationName;
  }
}
