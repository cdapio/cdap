/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;

/**
 * App with a Worker that throws an exception during initialize/run/destroy methods.
 */
public class AppWithExceptionThrowingWorker extends AbstractApplication {
  public static final String WORKER_NAME = "ExceptionThrowingWorker";
  public static final String INITIALIZE = "init";
  public static final String RUN = "run";
  public static final String DESTROY = "destroy";

  @Override
  public void configure() {
    addWorker(new ExceptionThrowingWorker());
  }

  /**
   * Worker which throws an exception in initialize, run, destroy methods in only in even numbered worker instance
   * and only when certain keys are present.
   */
  public static class ExceptionThrowingWorker extends AbstractWorker {

    @Override
    protected void configure() {
      super.configure();
      setName(WORKER_NAME);
      setInstances(1);
    }

    @Override
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      if (context.getInstanceId() % 2 == 1) {
        return;
      }

      if (context.getRuntimeArguments().containsKey(INITIALIZE)) {
        throw new RuntimeException("Throwing exception in initialize");
      }
    }

    @Override
    public void run() {
      if (getContext().getInstanceId() % 2 == 1) {
        return;
      }

      if (getContext().getRuntimeArguments().containsKey(RUN)) {
        throw new RuntimeException("Throwing exception in run method");
      }
    }

    @Override
    public void destroy() {
      if (getContext().getInstanceId() % 2 == 1) {
        return;
      }

      if (getContext().getRuntimeArguments().containsKey(DESTROY)) {
        throw new RuntimeException("Throwing exception in destroy");
      }
    }
  }
}
