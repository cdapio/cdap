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
 * Application to test pause and resume functionality for the Workflow
 */
public class PauseResumeWorklowApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("PauseResumeWorkflowApp");
    setDescription("Workflow app to test pause and resume functionality");
    addWorkflow(new PauseResumeWorkflow());
  }

  static final class PauseResumeWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      setName("PauseResumeWorkflow");
      setDescription("Workflow to pause and resume");
      addAction(new FirstSimpleAction());
      fork()
        .addAction(new ForkedSimpleAction())
      .also()
        .addAction(new AnotherForkedSimpleAction())
      .join();
      addAction(new LastSimpleAction());
    }
  }

  static final class FirstSimpleAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(FirstSimpleAction.class);
    @Override
    public void run() {
      LOG.info("Running FirstSimpleAction");
      try {
        File file = new File(getContext().getRuntimeArguments().get("first.simple.action.file"));
        file.createNewFile();
        File doneFile = new File(getContext().getRuntimeArguments().get("first.simple.action.donefile"));
        while (!doneFile.exists()) {
          TimeUnit.SECONDS.sleep(1);
        }
      } catch (Exception e) {
        // no-op
      }
    }
  }

  static final class ForkedSimpleAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(ForkedSimpleAction.class);
    @Override
    public void run() {
      try {
        LOG.info("Running ForkedSimpleAction");
        File file = new File(getContext().getRuntimeArguments().get("forked.simple.action.file"));
        file.createNewFile();
        File doneFile = new File(getContext().getRuntimeArguments().get("forked.simple.action.donefile"));
        while (!doneFile.exists()) {
          TimeUnit.SECONDS.sleep(1);
        }
      } catch (Exception e) {
        // no-op
      }
    }
  }

  static final class AnotherForkedSimpleAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(AnotherForkedSimpleAction.class);
    @Override
    public void run() {
      try {
        LOG.info("Running AnotherForkedSimpleAction");
        File file = new File(getContext().getRuntimeArguments().get("anotherforked.simple.action.file"));
        file.createNewFile();
        File doneFile = new File(getContext().getRuntimeArguments().get("anotherforked.simple.action.donefile"));
        while (!doneFile.exists()) {
          TimeUnit.SECONDS.sleep(1);
        }
      } catch (Exception e) {
        // no-op
      }
    }
  }

  static final class LastSimpleAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(LastSimpleAction.class);
    @Override
    public void run() {
      LOG.info("Running LastSimpleAction");
      try {
        File file = new File(getContext().getRuntimeArguments().get("last.simple.action.file"));
        file.createNewFile();
        File doneFile = new File(getContext().getRuntimeArguments().get("last.simple.action.donefile"));
        while (!doneFile.exists()) {
          TimeUnit.SECONDS.sleep(1);
        }
      } catch (Exception e) {
        // no-op
      }
    }
  }
}
