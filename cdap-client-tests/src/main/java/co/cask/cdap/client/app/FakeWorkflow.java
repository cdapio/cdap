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

package co.cask.cdap.client.app;

import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowConfigurer;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FakeWorkflow implements Workflow {

  public static final String NAME = "FakeWorkflow";

  @Override
  public void configure(WorkflowConfigurer configurer) {
    configurer.setName(NAME);
    configurer.addAction(new FakeAction());
  }

  /**
   * DummyAction
   */
  public static class FakeAction extends AbstractWorkflowAction {
    @Override
    public void run() {
      File doneFile = new File("/tmp/fakeworkflow.done");
      while (!doneFile.exists()) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          // no-op
        }
      }
    }
  }
}
