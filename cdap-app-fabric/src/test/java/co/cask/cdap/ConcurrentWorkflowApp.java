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
 * App to test multiple concurrent runs of a single workflow.
 */
public class ConcurrentWorkflowApp extends AbstractApplication {
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentWorkflowApp.class);
  public static final String FILE_TO_CREATE_ARG = "file.to.create";
  public static final String DONE_FILE_ARG = "done.file";

  @Override
  public void configure() {
    setDescription("Application with concurrently running Workflow instances");
    addWorkflow(new ConcurrentWorkflow());
  }

  /**
   * Workflow with a single action to test concurrent runs.
   */
  public static class ConcurrentWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setDescription("Workflow configured to run concurrently.");
      addAction(new SimpleAction());
    }
  }

  /**
   * Custom action that creates a file passed as a runtime argument and then waits for a done file to be created
   * externally.
   */
  public static final class SimpleAction extends AbstractCustomAction {
    @Override
    public void run() {
      Map<String, String> runtimeArguments = getContext().getRuntimeArguments();
      File file = new File(runtimeArguments.get(FILE_TO_CREATE_ARG));
      LOG.info("Creating file - " + file);
      try {
        Preconditions.checkArgument(file.createNewFile());
      } catch (IOException e) {
        LOG.error("Exception while creating file {}", file, e);
        throw Throwables.propagate(e);
      }
      File doneFile = new File(runtimeArguments.get(DONE_FILE_ARG));

      while (!doneFile.exists()) {
        try {
          TimeUnit.MILLISECONDS.sleep(50);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for done file.");
          Thread.currentThread().interrupt();
        }
      }
      LOG.info("Found done file {}. Workflow completed.", doneFile);
    }
  }
}
