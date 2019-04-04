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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerInfoCodec;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import org.apache.twill.api.RunId;

/**
 * Helper class to encoded/decode {@link io.cdap.cdap.api.schedule.TriggeringScheduleInfo} to/from json.
 */
public class TriggeringScheduleInfoAdapter {

  public static GsonBuilder addTypeAdapters(GsonBuilder builder) {
    return ApplicationSpecificationAdapter.addTypeAdapters(builder)
      .registerTypeAdapter(TriggerInfo.class, new TriggerInfoCodec())
      .registerTypeAdapter(RunId.class, new RunIds.RunIdCodec())
      .registerTypeAdapterFactory(new TypeAdapterFactory() {
        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
          if (TriggeringScheduleInfo.class.equals(type.getRawType())) {
            //noinspection unchecked
            return (TypeAdapter<T>) gson.getAdapter(DefaultTriggeringScheduleInfo.class);
          }
          if (WorkflowToken.class.equals(type.getRawType())) {
            //noinspection unchecked
            return (TypeAdapter<T>) gson.getAdapter(BasicWorkflowToken.class);
          }
          return null;
        }
      });
  }
}
