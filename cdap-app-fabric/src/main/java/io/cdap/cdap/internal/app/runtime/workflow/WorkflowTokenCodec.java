/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.workflow;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import java.lang.reflect.Type;

/**
 * Deserializer for {@link WorkflowToken}
 */
public class WorkflowTokenCodec implements JsonDeserializer<WorkflowToken> {

  @Override
  public WorkflowToken deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    return jsonDeserializationContext.deserialize(jsonElement, new TypeToken<BasicWorkflowToken>() {
    }.getType());
  }
}
