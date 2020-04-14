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

package io.cdap.cdap.internal.app.runtime;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A {@link ProgramController} implementation that control a guava Service.
 * The Service must execute Listeners in the order they were added to the Service, otherwise
 * there may be race conditions if the thread that stops the controller is different than the thread
 * that runs the service. Any Service that extends AbstractService meets this criteria.
 */
public class ProgramControllerServiceAdapter extends AbstractProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramControllerServiceAdapter.class);
  private static final Logger USERLOG = Loggers.mdcWrapper(LOG, Constants.Logging.EVENT_TYPE_TAG,
                                                           Constants.Logging.USER_LOG_TAG_VALUE);

  private final Service service;
  private final CountDownLatch serviceStoppedLatch;

  public ProgramControllerServiceAdapter(Service service, ProgramRunId programRunId) {
    super(programRunId);
    this.service = service;
    this.serviceStoppedLatch = new CountDownLatch(1);
    listenToRuntimeState(service);
  }

  @Override
  protected void doSuspend() throws Exception {

  }

  @Override
  protected void doResume() throws Exception {

  }

  @Override
  protected void doStop() throws Exception {
    if (service.state() != Service.State.TERMINATED && service.state() != Service.State.FAILED) {
      LOG.debug("stopping controller service for program {}.", getProgramRunId());
      service.stopAndWait();
      LOG.debug("stopped controller service for program {}, waiting for it to finish running listener hooks.",
                getProgramRunId());
      serviceStoppedLatch.await(30, TimeUnit.SECONDS);
      LOG.debug("controller service for program {} finished running listener hooks.", getProgramRunId());
    }
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {

  }

  private void listenToRuntimeState(Service service) {
    service.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        started();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        Throwable rootCause = Throwables.getRootCause(failure);
        LOG.error("{} Program '{}' failed.", getProgramRunId().getType(), getProgramRunId().getProgram(), failure);
        USERLOG.error("{} program '{}' failed with error: {}. Please check the system logs for more details.",
                      getProgramRunId().getType(), getProgramRunId().getProgram(), rootCause.getMessage(), rootCause);
        serviceStoppedLatch.countDown();
        error(failure);
      }

      @Override
      public void terminated(Service.State from) {
        serviceStoppedLatch.countDown();
        if (from != Service.State.STOPPING) {
          // Service completed by itself. Simply signal the state change of this controller.
          complete();
        } else {
          // Service was killed
          stop();
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }
}
