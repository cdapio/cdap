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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Codec to serialize and deserialize {@link WorkflowNode}
 */
final class WorkflowNodeCodec extends AbstractSpecificationCodec<WorkflowNode> {
  @Override
  public JsonElement serialize(WorkflowNode src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.add("nodeId", new JsonPrimitive(src.getNodeId()));
    jsonObj.add("nodeType", new JsonPrimitive(src.getType().toString()));

    switch (src.getType()) {
      case ACTION:
        serializeActionNode(src, jsonObj, context);
        break;
      case FORK:
        serializeForkNode(src, jsonObj, context);
        break;
      case CONDITION:
        // no-op
        break;
      default:
        // no-ops
        break;
    }
    return jsonObj;
  }

  private void serializeActionNode(WorkflowNode node, JsonObject jsonObj, JsonSerializationContext context) {
    WorkflowActionNode actionNode = (WorkflowActionNode) node;
    jsonObj.add("program", context.serialize(actionNode.getProgram(), ScheduleProgramInfo.class));
    if (actionNode.getProgram().getProgramType() == SchedulableProgramType.CUSTOM_ACTION) {
      jsonObj.add("actionSpecification", context.serialize(actionNode.getActionSpecification(),
                                                           WorkflowActionSpecification.class));
    }
  }

  private void serializeForkNode(WorkflowNode node, JsonObject jsonObj, JsonSerializationContext context) {
    WorkflowForkNode forkNode = (WorkflowForkNode) node;
    Type type = new TypeToken<List<List<WorkflowNode>>>() { }.getType();
    jsonObj.add("branches", context.serialize(forkNode.getBranches(), type));
  }

  @Override
  public WorkflowNode deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String nodeId = context.deserialize(jsonObj.get("nodeId"), String.class);
    WorkflowNodeType type = context.deserialize(jsonObj.get("nodeType"), WorkflowNodeType.class);

    WorkflowNode node = null;
    switch (type) {
      case ACTION:
        node = createActionNode(nodeId, jsonObj, context);
        break;
      case FORK:
        node = createForkNode(nodeId, jsonObj, context);
        break;
      case CONDITION:
        // no-op
        break;
      default:
        break;
    }
    return node;
  }

  private WorkflowNode createActionNode(String nodeId, JsonObject jsonObj, JsonDeserializationContext context) {
    ScheduleProgramInfo program = context.deserialize(jsonObj.get("program"), ScheduleProgramInfo.class);
    if (program.getProgramType() == SchedulableProgramType.CUSTOM_ACTION) {
      WorkflowActionSpecification actionSpecification = context.deserialize(jsonObj.get("actionSpecification"),
                                                                            WorkflowActionSpecification.class);
      return new WorkflowActionNode(nodeId, actionSpecification);
    } else {
      return new WorkflowActionNode(nodeId, program);
    }
  }

  private WorkflowNode createForkNode(String nodeId, JsonObject jsonObj, JsonDeserializationContext context) {
    Type type = new TypeToken<List<List<WorkflowNode>>>() { }.getType();
    List<List<WorkflowNode>> branches =  context.deserialize(jsonObj.get("branches"), type);
    return new WorkflowForkNode(nodeId, branches);
  }
}
