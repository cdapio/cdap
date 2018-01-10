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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.WebCrawlApp;
import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.api.schedule.TriggeringScheduleInfo;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.deploy.Specifications;
import co.cask.cdap.internal.app.runtime.schedule.trigger.DefaultPartitionTriggerInfo;
import co.cask.cdap.internal.app.runtime.schedule.trigger.DefaultProgramStatusTriggerInfo;
import co.cask.cdap.internal.app.runtime.schedule.trigger.DefaultTimeTriggerInfo;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Test serialize/deserialize TriggeringScheduleInfo
 */
public class TriggeringScheduleInfoAdapterTest {
  private static final Gson GSON = TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder()).create();

  @Test
  public void testSerDeserScheduleInfo() {
    BasicWorkflowToken token = new BasicWorkflowToken(1);
    token.setCurrentNode("node");
    token.put("tokenKey", "tokenVal");
    List<TriggerInfo> triggerInfos =
      ImmutableList.<TriggerInfo>of(
        new DefaultProgramStatusTriggerInfo("ns", Specifications.from(new WebCrawlApp()), ProgramType.WORKFLOW,
                                            "workflow", RunIds.generate(), ProgramStatus.COMPLETED,
                                            token, Collections.<String, String>emptyMap()),
        new DefaultPartitionTriggerInfo("ns", "ds", 10, 11),
        new DefaultTimeTriggerInfo("1 * * * *", 0L));
    TriggeringScheduleInfo scheduleInfo = new DefaultTriggeringScheduleInfo("schedule", "description", triggerInfos,
                                                                            ImmutableMap.of("key", "value"));

    String scheduleInfoJson = GSON.toJson(scheduleInfo);
    TriggeringScheduleInfo deserializedScheduleInfo = GSON.fromJson(scheduleInfoJson,
                                                                    TriggeringScheduleInfo.class);
    Assert.assertEquals(scheduleInfoJson, GSON.toJson(deserializedScheduleInfo));
    DefaultProgramStatusTriggerInfo expectedProgramStatusTriggerInfo =
      (DefaultProgramStatusTriggerInfo) triggerInfos.get(0);
    DefaultProgramStatusTriggerInfo deserializedProgramStatusTriggerInfo =
      (DefaultProgramStatusTriggerInfo) deserializedScheduleInfo.getTriggerInfos().get(0);
    Assert.assertEquals(expectedProgramStatusTriggerInfo.getApplicationSpecification().getName(),
                        deserializedProgramStatusTriggerInfo.getApplicationSpecification().getName());
    Assert.assertEquals(expectedProgramStatusTriggerInfo.getWorkflowToken().getAll(),
                        deserializedProgramStatusTriggerInfo.getWorkflowToken().getAll());
  }
}
