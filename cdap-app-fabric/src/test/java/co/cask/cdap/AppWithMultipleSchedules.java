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

package co.cask.cdap;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.schedule.ProgramStatusTriggerInfo;
import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.api.schedule.TriggeringScheduleInfo;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AppWithMultipleSchedules extends AbstractApplication {
  private static final Gson GSON = new Gson();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  public static final String NAME = "AppWithMultipleScheduledWorkflows";
  public static final String SOME_WORKFLOW = "SomeWorkflow";
  public static final String ANOTHER_WORKFLOW = "AnotherWorkflow";
  public static final String TRIGGERED_WORKFLOW = "TriggeredWorkflow";
  public static final String WORKFLOW_COMPLETED_SCHEDULE = "WorkflowCompletedSchedule2";
  public static final String ANOTHER_RUNTIME_ARG_KEY = "AnotherWorkflowRuntimeArgKey";
  public static final String ANOTHER_RUNTIME_ARG_VALUE = "AnotherWorkflowRuntimeArgValue";
  public static final String ANOTHER_TOKEN_KEY = "AnotherWorkflowTokenKey";
  public static final String ANOTHER_TOKEN_VALUE = "AnotherWorkflowTokenValue";
  public static final String TRIGGERED_RUNTIME_ARG_KEY = "TriggeredWorkflowRuntimeArgKey";
  public static final String TRIGGERED_TOKEN_KEY = "TriggeredWorkflowTokenKey";
  public static final String TRIGGERING_PROPERTIES_MAPPING = "triggering.properties.mapping";

  @Override
  public void configure() {
    setName("AppWithMultipleScheduledWorkflows");
    setDescription("Sample application with multiple Workflows");
    addWorkflow(new SomeWorkflow());
    addWorkflow(new AnotherWorkflow());
    addWorkflow(new TriggeredWorkflow());
    schedule(buildSchedule("SomeSchedule1", ProgramType.WORKFLOW, SOME_WORKFLOW).triggerByTime("0 4 * * *"));
    schedule(buildSchedule("SomeSchedule2", ProgramType.WORKFLOW, SOME_WORKFLOW).triggerByTime("0 5 * * *"));
    schedule(buildSchedule("AnotherSchedule1", ProgramType.WORKFLOW, ANOTHER_WORKFLOW).triggerByTime("0 6 * * *"));
    schedule(buildSchedule("AnotherSchedule2", ProgramType.WORKFLOW, ANOTHER_WORKFLOW).triggerByTime("0 7 * * *"));
    schedule(buildSchedule("AnotherSchedule3", ProgramType.WORKFLOW, ANOTHER_WORKFLOW).triggerByTime("0 8 * * *"));
    schedule(buildSchedule("TriggeredWorkflowSchedule", ProgramType.WORKFLOW, TRIGGERED_WORKFLOW)
               .triggerByTime("0 8 * * *"));
    schedule(buildSchedule("WorkflowCompletedSchedule1", ProgramType.WORKFLOW, TRIGGERED_WORKFLOW)
               .triggerOnProgramStatus(ProgramType.WORKFLOW, SOME_WORKFLOW, ProgramStatus.COMPLETED));
    schedule(buildSchedule("WorkflowFailedSchedule", ProgramType.WORKFLOW, TRIGGERED_WORKFLOW)
               .triggerOnProgramStatus(ProgramType.WORKFLOW, SOME_WORKFLOW, ProgramStatus.FAILED));
    schedule(buildSchedule("WorkflowCompletedFailedSchedule", ProgramType.WORKFLOW, TRIGGERED_WORKFLOW)
               .triggerOnProgramStatus(ProgramType.WORKFLOW, SOME_WORKFLOW,
                                       ProgramStatus.COMPLETED, ProgramStatus.FAILED));

    // Create a TRIGGERING_PROPERTIES_MAPPING which defines how runtime args and workflow tokens in ANOTHER_WORKFLOW
    // will be used by TRIGGERED_WORKFLOW as workflow token
    Map<String, String> triggeringPropertiesMap =
      // Use the value of runtime arg with key ANOTHER_RUNTIME_ARG_KEY in ANOTHER_WORKFLOW
      // as the value of the workflow token with key TRIGGERED_RUNTIME_ARG_KEY in TRIGGERED_WORKFLOW
      ImmutableMap.of(String.format("runtime-arg#%s", ANOTHER_RUNTIME_ARG_KEY), TRIGGERED_RUNTIME_ARG_KEY,
                      // Use the value of workflow token with key ANOTHER_TOKEN_KEY in ANOTHER_WORKFLOW
                      // as the value of the workflow token with key TRIGGERED_TOKEN_KEY in TRIGGERED_WORKFLOW
                      String.format("token#%s:%s", ANOTHER_TOKEN_KEY, ANOTHER_WORKFLOW), TRIGGERED_TOKEN_KEY);
    schedule(buildSchedule(WORKFLOW_COMPLETED_SCHEDULE, ProgramType.WORKFLOW, TRIGGERED_WORKFLOW)
               .setProperties(ImmutableMap.of(TRIGGERING_PROPERTIES_MAPPING, GSON.toJson(triggeringPropertiesMap)))
               .triggerOnProgramStatus(ProgramType.WORKFLOW, ANOTHER_WORKFLOW, ProgramStatus.COMPLETED));
  }

  /**
   * Some Workflow
   */
  public static class SomeWorkflow extends AbstractWorkflow {
    public static final String NAME = SOME_WORKFLOW;
    @Override
    public void configure() {
      setName(NAME);
      setDescription("SomeWorkflow description");
      addAction(new SomeDummyAction());
    }
  }

  /**
   * Some Dummy Action
   */
  public static class SomeDummyAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(SomeDummyAction.class);

    @Override
    public void run() {
      LOG.info("Ran some dummy action");
    }
  }

  /**
   * Another Workflow
   */
  public static class AnotherWorkflow extends AbstractWorkflow {
    public static final String NAME = ANOTHER_WORKFLOW;
    @Override
    public void configure() {
      setName(NAME);
      setDescription("AnotherWorkflow description");
      addAction(new AnotherDummyAction());
    }

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      context.getToken().put(ANOTHER_TOKEN_KEY, ANOTHER_TOKEN_VALUE);
    }
  }

  /**
   * Another Dummy Action
   */
  public static class AnotherDummyAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(AnotherDummyAction.class);

    @Override
    public void run() {
      LOG.info("Ran another dummy action");
    }
  }

  /**
   * Triggered Workflow
   */
  public static class TriggeredWorkflow extends AbstractWorkflow {
    public static final String NAME = TRIGGERED_WORKFLOW;
    @Override
    public void configure() {
      setName(NAME);
      setDescription("TriggeredWorkflow description");
      addAction(new SomeDummyAction());
    }
    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      TriggeringScheduleInfo scheduleInfo = context.getTriggeringScheduleInfo();
      if (scheduleInfo != null) {
        // Get values of the runtime args and workflow token from the triggering program
        // whose keys are defined as keys in TRIGGERING_PROPERTIES_MAPPING with a special syntax and use their values
        // for the corresponding workflow tokens as defined in TRIGGERING_PROPERTIES_MAPPING values
        String propertiesMappingString =
          scheduleInfo.getProperties().get(TRIGGERING_PROPERTIES_MAPPING);
        if (propertiesMappingString != null) {
          Map<String, String> propertiesMap = GSON.fromJson(propertiesMappingString, STRING_STRING_MAP);
          Map<String, String> newTokenMap =
            getNewTokensFromScheduleInfo(scheduleInfo, propertiesMap);
          for (Map.Entry<String, String> entry : newTokenMap.entrySet()) {
            // Write the workflow token into context
            context.getToken().put(entry.getKey(), entry.getValue());
          }
        }
      }
    }
  }

  private static Map<String, String> getNewTokensFromScheduleInfo(TriggeringScheduleInfo scheduleInfo,
                                                                  Map<String, String> propertiesMap) {
    List<TriggerInfo> triggerInfoList = scheduleInfo.getTriggerInfos();
    List<ProgramStatusTriggerInfo> programStatusTriggerInfos = new ArrayList<>();
    for (TriggerInfo info : triggerInfoList) {
      if (info instanceof ProgramStatusTriggerInfo) {
        programStatusTriggerInfos.add((ProgramStatusTriggerInfo) info);
      }
    }
    Map<String, String> newRuntimeArgs = new HashMap<>();
    // If no ProgramStatusTriggerInfo, no need of override the existing runtimeArgs
    if (programStatusTriggerInfos.size() == 0) {
      return newRuntimeArgs;
    }
    // The syntax for runtime args from the triggering program is:
    //   runtime-arg#<runtime-arg-key>
    // User tokens from the triggering pipeline:
    //   token#<user-token-key>:<node-name>
    for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
      String triggeringPropertyName = entry.getKey();
      String[] propertyParts = triggeringPropertyName.split("#");
      if ("runtime-arg".equals(propertyParts[0])) {
        addRuntimeArgs(programStatusTriggerInfos, newRuntimeArgs, propertyParts[1], entry.getValue());
      } else if ("token".equals(propertyParts[0])) {
        addTokens(programStatusTriggerInfos, newRuntimeArgs, propertyParts[1], entry.getValue());
      }
    }
    return newRuntimeArgs;
  }

  private static void addRuntimeArgs(List<ProgramStatusTriggerInfo> programStatusTriggerInfos,
                                     Map<String, String> runtimeArgs, String triggeringKey, String key) {
    for (ProgramStatusTriggerInfo triggerInfo : programStatusTriggerInfos) {
      Map<String, String> triggeringRuntimeArgs = triggerInfo.getRuntimeArguments();
      if (triggeringRuntimeArgs != null && triggeringRuntimeArgs.containsKey(triggeringKey)) {
        runtimeArgs.put(key, triggeringRuntimeArgs.get(triggeringKey));
      }
    }
  }

  private static void addTokens(List<ProgramStatusTriggerInfo> programStatusTriggerInfos,
                                Map<String, String> runtimeArgs, String triggeringKeyNodePair, String key) {
    for (ProgramStatusTriggerInfo triggerInfo : programStatusTriggerInfos) {
      WorkflowToken token = triggerInfo.getWorkflowToken();
      if (token == null) {
        continue;
      }
      String[] keyNode = triggeringKeyNodePair.split(":");
      Value value = token.get(keyNode[0], keyNode[1]);
      if (value == null) {
        continue;
      }
      runtimeArgs.put(key, value.toString());
    }
  }
}
