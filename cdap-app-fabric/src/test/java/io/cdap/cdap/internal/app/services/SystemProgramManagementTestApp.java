/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.customaction.CustomAction;
import io.cdap.cdap.api.workflow.AbstractWorkflow;

import java.util.concurrent.CountDownLatch;

/**
 * An application for testing {@link SystemProgramManagementService}.
 */
public class SystemProgramManagementTestApp extends AbstractApplication {

  public static final String NAME = SystemProgramManagementTestApp.class.getSimpleName();

  @Override
  public void configure() {
    addWorkflow(new IdleWorkflow());
  }

  public static final class IdleWorkflow extends AbstractWorkflow {

    public static final String NAME = IdleWorkflow.class.getSimpleName();

    @Override
    protected void configure() {
      addAction(new IdleAction());
    }
  }

  /**
   * A {@link CustomAction} that just stay idle until interrupted.
   */
  public static final class IdleAction extends AbstractCustomAction {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void run() throws Exception {
      try {
        // Just wait indefinitely until interrupted
        latch.await();
      } catch (InterruptedException e) {
        // Ignore
      }
    }
  }
}
