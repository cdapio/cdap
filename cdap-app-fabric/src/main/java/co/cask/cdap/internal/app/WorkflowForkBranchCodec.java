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

import co.cask.cdap.api.workflow.WorkflowForkBranch;
import co.cask.cdap.api.workflow.WorkflowNode;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.List;

/**
 *
 */
final class WorkflowForkBranchCodec extends AbstractSpecificationCodec<WorkflowForkBranch> {

  @Override
  public JsonElement serialize(WorkflowForkBranch src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.add("branch", serializeList(src.getNodes(), context, WorkflowNode.class));
    return jsonObj;
  }

  @Override
  public WorkflowForkBranch deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    List<WorkflowNode> nodes = deserializeList(jsonObj.get("branch"), context, WorkflowNode.class);
    return new WorkflowForkBranch(nodes);
  }
}
