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

import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowForkBranch;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import com.google.common.base.Preconditions;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.List;

/**
 *
 */
final class WorkflowNodeCodec extends AbstractSpecificationCodec<WorkflowNode> {
  @Override
  public JsonElement serialize(WorkflowNode src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.add("nodeId", new JsonPrimitive(src.getNodeId()));
    jsonObj.add("nodeType", new JsonPrimitive(src.getType().toString()));

    switch (src.getType()) {
      case ACTION:
        WorkflowActionNode actionNode = (WorkflowActionNode) src;
        jsonObj.add("program", context.serialize(actionNode.getProgram(), ScheduleProgramInfo.class));
        break;
      case FORK:
        WorkflowForkNode forkNode = (WorkflowForkNode) src;
        jsonObj.add("branches", serializeList(forkNode.getBranches(), context, WorkflowForkBranch.class));
        break;
      case CONDITION:
        // no-op
        break;
      default:
        // no-op
        break;
    }
    return jsonObj;
  }

  @Override
  public WorkflowNode deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    List<WorkflowForkBranch> branches = null;
    ScheduleProgramInfo program = null;
    WorkflowNode node = null;

    String nodeId = context.deserialize(jsonObj.get("nodeId"), String.class);
    WorkflowNodeType type = context.deserialize(jsonObj.get("nodeType"), WorkflowNodeType.class);

    switch (type) {
      case ACTION:
        program = context.deserialize(jsonObj.get("program"), ScheduleProgramInfo.class);
        node = new WorkflowActionNode(nodeId, program);
        break;
      case FORK:
        branches =  deserializeList(jsonObj.get("branches"), context, WorkflowForkBranch.class);
        node = new WorkflowForkNode(nodeId, branches);
        break;
      case CONDITION:
        // no-op
        break;
      default:
        break;
    }
    return node;
  }
}
