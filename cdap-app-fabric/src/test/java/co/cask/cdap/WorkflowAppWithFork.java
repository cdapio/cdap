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
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class WorkflowAppWithFork extends AbstractApplication {
  @Override
  public void configure() {
    setDescription("Workflow App containing fork.");
    addWorkflow(new WorkflowWithFork());
  }

  /**
   *
   */
  public static class WorkflowWithFork extends AbstractWorkflow {

    @Override
    public void configure() {
      setDescription("A workflow that tests forks.");
      addAction(new SimpleAction("first"));
      fork()
        .addAction(new SimpleAction("branch1"))
      .also()
        .addAction(new SimpleAction("branch2"))
      .join();
    }
  }

  static final class SimpleAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleAction.class);

    SimpleAction(String name) {
      super(name);
    }

    @Override
    public void run() {
      String name = getContext().getSpecification().getName();
      LOG.info("Running SimpleAction: {}", name);
      Map<String, String> runtimeArguments = getContext().getRuntimeArguments();
      File file = new File(runtimeArguments.get(name + ".file"));
      try {
        Preconditions.checkArgument(file.createNewFile(), "Failed to create file '%s'", file);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      File doneFile = new File(runtimeArguments.get(name + ".donefile"));
      try {
        while (!doneFile.exists()) {
          TimeUnit.MILLISECONDS.sleep(50);
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for done file {}", doneFile);
        Thread.currentThread().interrupt();
      }
      LOG.info("Done file '{}' exists. Simple Action '{}' completed.", doneFile, name);
    }
  }
}
