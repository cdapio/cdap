/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Codec to serialize/deserialize {@link WorkflowTokenNodeDetail}.
 */
public class WorkflowTokenNodeDetailCodec extends AbstractSpecificationCodec<WorkflowTokenNodeDetail> {

  @Override
  public JsonElement serialize(WorkflowTokenNodeDetail src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src.getTokenDataAtNode());
  }

  @Override
  public WorkflowTokenNodeDetail deserialize(JsonElement json, Type typeOfT,
                                             JsonDeserializationContext context) throws JsonParseException {
    Map<String, String> tokenDataAtNode = deserializeMap(json, context, String.class);
    return new WorkflowTokenNodeDetail(tokenDataAtNode);
  }
}
