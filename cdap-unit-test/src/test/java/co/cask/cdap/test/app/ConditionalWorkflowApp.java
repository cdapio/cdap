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

package co.cask.cdap.test.app;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.workflow.AbstractCondition;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Application to test the Conditions in the Workflow.
 */
public class ConditionalWorkflowApp extends AbstractApplication {
  @Override
  public void configure() {
    addWorkflow(new ConditionalWorkflow());
  }

  /**
   * Workflow with conditions
   */
  public static class ConditionalWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      condition(new SimpleCondition())
        .addAction(new MyCustomAction("action1"))
      .otherwise()
        .condition(new ConfigurableCondition())
          .addAction(new MyCustomAction("action2"))
        .otherwise()
          .addAction(new MyCustomAction("action3"))
        .end()
      .end();
    }
  }

  /**
   * Simple condition
   */
  public static class SimpleCondition implements Predicate<WorkflowContext> {

    @Override
    public boolean apply(@Nullable WorkflowContext input) {
      input.getToken().put("simple.condition.initialize", "true");
      if (input.getRuntimeArguments().containsKey("simple.condition")) {
        return true;
      }
      return false;
    }
  }

  /**
   * Configurable condition
   */
  public static class ConfigurableCondition extends AbstractCondition {
    @Override
    protected void configure() {
      super.configure();
      setName("MyConfigurableCondition");
      setDescription("This is configurable conditions description.");
      Map<String, String> properties = ImmutableMap.of("prop1", "value1", "prop2", "value2", "prop3", "value3");
      setProperties(properties);
    }

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      getContext().getToken().put("configurable.condition.initialize", "true");
    }

    @Override
    public void destroy() {
      super.destroy();
      getContext().getToken().put("configurable.condition.destroy", "true");
    }

    @Override
    public boolean apply(@Nullable WorkflowContext input) {
      input.getToken().put("configurable.condition.apply", "true");
      if (input.getRuntimeArguments().containsKey("configurable.condition")) {
        return true;
      }
      return false;
    }
  }

  /**
   *
   */
  public static class MyCustomAction extends AbstractCustomAction {

    private final String name;

    public MyCustomAction(String name) {
      this.name = name;
    }

    @Override
    protected void configure() {
      super.configure();
      setName(name);
    }

    @Override
    public void run() throws Exception {
      WorkflowToken token = getContext().getWorkflowToken();
      token.put("action.name", getContext().getSpecification().getName());
    }
  }
}
