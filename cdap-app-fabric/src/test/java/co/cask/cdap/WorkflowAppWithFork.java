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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class WorkflowAppWithFork extends AbstractApplication {
  @Override
  public void configure() {
    setName("WorkflowAppWithFork");
    setDescription("Workflow App containing fork");
    addWorkflow(new WorkflowWithFork());
  }

  /**
   *
   */
  public static class WorkflowWithFork extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("WorkflowWithFork");
      setDescription("WorkflowWithFork description");
      addAction(new SimpleAction("first"));
      fork()
        .addAction(new SimpleAction("one"))
      .also()
        .addAction(new SimpleAction("another"))
      .join();
    }
  }

  static final class SimpleAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleAction.class);

    public SimpleAction(String name) {
      super(name);
    }

    @Override
    public void run() {
      LOG.info("Running SimpleAction: " + getContext().getSpecification().getName());
      try {
        File file = new File(getContext().getRuntimeArguments().get(getContext().getSpecification().getName() +
                                                                      ".simple.action.file"));
        file.createNewFile();
        File doneFile = new File(getContext().getRuntimeArguments().get(getContext().getSpecification().getName() +
                                                                          ".simple.action.donefile"));
        while (!doneFile.exists()) {
          TimeUnit.MILLISECONDS.sleep(50);
        }
      } catch (Exception e) {
        // no-op
      }
    }
  }
}
