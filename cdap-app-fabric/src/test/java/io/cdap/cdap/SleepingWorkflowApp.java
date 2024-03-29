/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap;

import io.cdap.cdap.api.annotation.Property;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple workflow, that sleeps inside a CustomAction, This class is used for testing the workflow status.
 */
public class SleepingWorkflowApp extends AbstractApplication {

  public static final String NAME = "SleepWorkflowApp";
  public static final String DESC = "SleepWorkflowApp";

  @Override
  public void configure() {
    setName(NAME);
    setDescription(DESC);
    addWorkflow(new SleepWorkflow());
  }

  /**
   *
   */
  public static class SleepWorkflow extends AbstractWorkflow {
    public static final String NAME = "SleepWorkflow";

    @Override
    public void configure() {
      setName(NAME);
      setDescription("FunWorkflow description");
      addAction(new CustomAction("verify"));
    }
  }

  /**
   *
   */
  public static final class CustomAction extends AbstractCustomAction {

    private static final Logger LOG = LoggerFactory.getLogger(CustomAction.class);

    private final String name;

    @Property
    private final boolean condition = true;

    public CustomAction(String name) {
      this.name = name;
    }

    @Override
    public void configure() {
      setName(name);
      setDescription(name);
    }

    @Override
    public void initialize() throws Exception {
      LOG.info("Custom action initialized: " + getContext().getSpecification().getName());
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
        String sleepTime = getContext().getRuntimeArguments().get("sleep.ms");
        Thread.sleep(sleepTime == null ? 2000 : Long.parseLong(sleepTime));
      } catch (InterruptedException e) {
        // expected if the workflow get killed
      }
      LOG.info("Custom run completed.");
    }
  }

}
