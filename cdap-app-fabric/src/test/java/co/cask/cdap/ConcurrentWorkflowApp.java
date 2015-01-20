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

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ConcurrentWorkflowApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("ConcurrentWorkflowApp");
    setDescription("Application with concurrently running Workflow instances");
    addWorkflow(new ConcurrentWorkflow());
    addWorkflow(new SequentialWorkflow());
    // Schedule Workflow to run every minute
    scheduleWorkflow("concurrentWorkflowSchedule1", "* * * * *", "ConcurrentWorkflow");
    scheduleWorkflow("concurrentWorkflowSchedule2", "* * * * *", "ConcurrentWorkflow");
    scheduleWorkflow("sequentialWorkflowSchedule1", "* * * * *", "SequentialWorkflow");
    scheduleWorkflow("sequentialWorkflowSchedule2", "* * * * *", "SequentialWorkflow");
  }

  /**
   *
   */
  private static class ConcurrentWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("ConcurrentWorkflow");
      setDescription("Workflow configured to run concurrently.");
      addAction(new SleepAction());
    }
  }

  /**
   *
   */
  private static class SequentialWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("SequentialWorkflow");
      setDescription("Workflow configured to run sequentially.");
      addAction(new SleepAction());
    }
  }

  /**
   * NoSleepAction
   */
  public static class NoSleepAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(NoSleepAction.class);
    @Override
    public void run() {
      LOG.info("Ran NoSleep action");
    }
  }

  /**
   * SleepAction
   */
  public static class SleepAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(SleepAction.class);
    @Override
    public void run() {
      LOG.info("Ran Sleep action");
      try {
        TimeUnit.SECONDS.sleep(30);
      } catch (InterruptedException ex) {
        LOG.info("Interrupted");
      }
    }
  }

}
