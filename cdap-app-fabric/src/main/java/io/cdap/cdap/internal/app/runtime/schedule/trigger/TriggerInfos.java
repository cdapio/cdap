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

package io.cdap.cdap.internal.app.runtime.schedule.trigger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.DefaultTriggeringScheduleInfo;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowTokenCodec;
import io.cdap.cdap.spi.events.StartMetadata;
import io.cdap.cdap.spi.events.StartType;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.twill.api.RunId;

/**
 * Helper functions for {@link TriggerInfo}
 */
public class TriggerInfos {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(RunId.class, new RunIds.RunIdCodec())
      .registerTypeAdapter(TriggerInfo.class, new TriggerInfoCodec())
      .registerTypeAdapter(WorkflowToken.class, new WorkflowTokenCodec())
      .create();

  private TriggerInfos() {

  }

  /**
   * Deserialize {@link TriggeringScheduleInfo} if SystemArgs contains the key
   * 'triggeringScheduleInfo' else returns null
   *
   * @return {@link TriggeringScheduleInfo}
   */
  @Nullable
  public static TriggeringScheduleInfo getTriggeringScheduleInfo(Map<String, String> sysArgs) {
    if (!sysArgs.containsKey(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO)) {
      return null;
    }
    // since only implementation we use is `DefaultTriggeringScheduleInfo`
    return GSON.fromJson(
        sysArgs.get(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO),
        DefaultTriggeringScheduleInfo.class);
  }

  /**
   * Deserialize {@link Trigger.Type} if SystemArgs contains the key 'triggeringScheduleInfoType'
   * else returns null
   *
   * @return {@link Trigger.Type}
   */
  @Nullable
  public static Trigger.Type getTriggerType(Map<String, String> sysArgs) {
    if (!sysArgs.containsKey(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO_TYPE)) {
      return null;
    }
    return GSON.fromJson(sysArgs.get(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO_TYPE),
        Trigger.Type.class);
  }

  /**
   * Creates a new {@link StartMetadata} from System Arguments, if sysArgs is null then returns
   * null
   *
   * @return {@link StartMetadata}
   */
  @Nullable
  public static StartMetadata getStartMetadata(Map<String, String> sysArgs) {
    if (sysArgs == null) {
      return null;
    }
    TriggeringScheduleInfo triggeringScheduleInfo = TriggerInfos.getTriggeringScheduleInfo(sysArgs);
    if (triggeringScheduleInfo == null) {
      return new StartMetadata(StartType.MANUAL, null);
    }
    Trigger.Type type = TriggerInfos.getTriggerType(sysArgs);
    return new StartMetadata(StartType.valueOfCategoryName(type.getCategoryName()),
        triggeringScheduleInfo);
  }
}
