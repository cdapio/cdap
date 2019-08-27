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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import com.google.common.util.concurrent.Uninterruptibles;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A testing app for testing concurrent run support
 */
public class ConcurrentRunTestApp extends AbstractApplication {
  @Override
  public void configure() {
    addWorker(new TestWorker());
    addWorkflow(new TestWorkflow());
  }

  /**
   * Test worker
   */
  public static final class TestWorker extends AbstractWorker {

    private final CountDownLatch stopLatch = new CountDownLatch(1);

    @Override
    public void run() {
      Uninterruptibles.awaitUninterruptibly(stopLatch);
    }

    @Override
    public void stop() {
      stopLatch.countDown();
    }
  }

  /**
   * Test Workflow
   */
  public static final class TestWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      addAction(new TestAction());
    }
  }

  /**
   * Test action
   */
  public static final class TestAction extends AbstractCustomAction {

    @Override
    public void run() throws Exception {
      // Poll until a file path exists
      boolean exists = false;
      while (!exists) {
        exists = new File(getContext().getRuntimeArguments().get("action.file")).exists();
        TimeUnit.MILLISECONDS.sleep(500);
      }
    }
  }
}
