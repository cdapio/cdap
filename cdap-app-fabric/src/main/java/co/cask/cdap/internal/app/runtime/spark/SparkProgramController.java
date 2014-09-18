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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.internal.app.runtime.AbstractProgramController;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ProgramController} for {@link Spark} jobs. This class acts as an adapter for reflecting state changes
 * happening in {@link SparkRuntimeService}
 */
public final class SparkProgramController extends AbstractProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(SparkProgramController.class);

  private final Service sparkRuntimeService;
  private final SparkContext context;

  SparkProgramController(Service sparkRuntimeService, BasicSparkContext context) {
    super(context.getProgramName(), context.getRunId());
    this.sparkRuntimeService = sparkRuntimeService;
    this.context = context;
    listenToRuntimeState(sparkRuntimeService);
  }

  @Override
  protected void doSuspend() throws Exception {
    // No-op
  }

  @Override
  protected void doResume() throws Exception {
    // No-op
  }

  @Override
  protected void doStop() throws Exception {
    if (sparkRuntimeService.state() != Service.State.TERMINATED &&
      sparkRuntimeService.state() != Service.State.FAILED) {
      sparkRuntimeService.stopAndWait();
    }
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    // No-op
  }

  /**
   * Returns the {@link SparkContext} for Spark run represented by this controller.
   */
  public SparkContext getContext() {
    return context;
  }

  private void listenToRuntimeState(Service service) {
    service.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        started();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.error("Spark terminated with exception", failure);
        error(failure);
      }

      @Override
      public void terminated(Service.State from) {
        if (getState() != State.STOPPING) {
          // Spark completed by itself. Simply signal the controller about the state change
          stop();
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }
}
