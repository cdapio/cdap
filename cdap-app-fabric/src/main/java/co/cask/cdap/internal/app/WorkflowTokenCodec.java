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

import co.cask.cdap.api.workflow.NodeValueEntry;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.proto.codec.AbstractSpecificationCodec;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Codec to serialize and deserialize {@link WorkflowToken}.
 */
public class WorkflowTokenCodec extends AbstractSpecificationCodec<WorkflowToken> {
  @Override
  public JsonElement serialize(WorkflowToken src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src);
  }

  @SuppressWarnings("unchecked")
  @Override
  public WorkflowToken deserialize(JsonElement json, Type typeOfT,
                                   JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    Type mapReduceCounterType = new TypeToken<Map<String, Map<String, Long>>>() { }.getType();
    Map<String, Map<String, Long>> mapReduceCounters = context.deserialize(jsonObj.get("mapReduceCounters"),
                                                                           mapReduceCounterType);

    String nodeName = jsonObj.get("nodeName").getAsString();

    Table<WorkflowToken.Scope, String, List<NodeValueEntry>> tokenValueMap = HashBasedTable.create();
    JsonElement tableBackingMap = ((JsonObject) jsonObj.get("tokenValueMap")).get("backingMap");
    if (tableBackingMap != null) {
      for (Map.Entry<String, JsonElement> entry : ((JsonObject) tableBackingMap).entrySet()) {
        WorkflowToken.Scope row = WorkflowToken.Scope.valueOf(entry.getKey());
        for (Map.Entry<String, JsonElement> column : ((JsonObject) entry.getValue()).entrySet()) {
          Type listType = new TypeToken<List<NodeValueEntry>>() {
          }.getType();
          List<NodeValueEntry> nodeValues = context.deserialize(column.getValue(), listType);
          tokenValueMap.put(row, column.getKey(), nodeValues);
        }
      }
    }

    return new BasicWorkflowToken(tokenValueMap, nodeName, mapReduceCounters);
  }
}
