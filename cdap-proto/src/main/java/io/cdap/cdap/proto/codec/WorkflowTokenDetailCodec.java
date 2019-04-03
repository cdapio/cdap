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
package co.cask.cdap.proto.codec;

import co.cask.cdap.proto.WorkflowTokenDetail;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Codec to serialize/deserialize {@link WorkflowTokenDetail}.
 */
public class WorkflowTokenDetailCodec extends AbstractSpecificationCodec<WorkflowTokenDetail> {

  @Override
  public JsonElement serialize(WorkflowTokenDetail src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src.getTokenData());
  }

  @Override
  public WorkflowTokenDetail deserialize(JsonElement json, Type typeOfT,
                                         JsonDeserializationContext context) throws JsonParseException {
    Map<String, List<WorkflowTokenDetail.NodeValueDetail>> tokenData = new HashMap<>();
    for (Map.Entry<String, JsonElement> entry : json.getAsJsonObject().entrySet()) {
      List<WorkflowTokenDetail.NodeValueDetail> nodeValueDetails = deserializeList(entry.getValue(), context,
                                                                       WorkflowTokenDetail.NodeValueDetail.class);
      tokenData.put(entry.getKey(), nodeValueDetails);
    }
    return new WorkflowTokenDetail(tokenData);
  }
}
