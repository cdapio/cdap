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

package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import org.apache.twill.api.RunId;

import java.lang.reflect.Type;

/**
 * Helper class to encoded/decode {@link co.cask.cdap.api.schedule.TriggeringScheduleInfo} to/from json.
 */
public class TriggeringScheduleInfoAdapter {

  public static GsonBuilder addTypeAdapters(GsonBuilder builder) {
    return ApplicationSpecificationAdapter.addTypeAdapters(builder)
      .registerTypeAdapter(TriggerInfo.class, new TriggerInfoCodec())
      .registerTypeAdapter(RunId.class, new RunIds.RunIdCodec())
      .registerTypeAdapter(WorkflowToken.class, new JsonDeserializer<WorkflowToken>() {
        @Override
        public WorkflowToken deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
          throws JsonParseException {
          return context.deserialize(json, BasicWorkflowToken.class);
        }
      });
  }
}
