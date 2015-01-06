/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Simple workflow, that sleeps inside a CustomAction, This class is used for testing the workflow status.
 */
public class SleepingWorkflowApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("SleepWorkflowApp");
    setDescription("SleepWorkflowApp");
    addWorkflow(new SleepWorkflow());
  }

  /**
   *
   */
  public static class SleepWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
        setName("SleepWorkflow");
        setDescription("FunWorkflow description");
        addAction(new CustomAction("verify"));
    }
  }


  /**
   *
   */
  public static final class CustomAction extends AbstractWorkflowAction {

    private static final Logger LOG = LoggerFactory.getLogger(CustomAction.class);

    private final String name;

    @Property
    private final boolean condition = true;

    public CustomAction(String name) {
      this.name = name;
    }

    @Override
    public WorkflowActionSpecification configure() {
      return WorkflowActionSpecification.Builder.with()
        .setName(name)
        .setDescription(name)
        .build();
    }

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      LOG.info("Custom action initialized: " + context.getSpecification().getName());
    }

    @Override
    public void destroy() {
      super.destroy();
      LOG.info("Custom action destroyed: " + getContext().getSpecification().getName());
    }

    @Override
    public void run() {
      LOG.info("Custom action run");
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      LOG.info("Custom run completed.");
    }
  }

}
