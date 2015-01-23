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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.Threads;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Stream service meant to run in an HTTP service.
 */
public abstract class AbstractStreamService extends AbstractScheduledService implements StreamService {

  private final StreamCoordinatorClient streamCoordinatorClient;
  private final StreamFileJanitorService janitorService;

  private ScheduledExecutorService executor;

  /**
   * Children classes should implement this method to add logic to the start of this {@link Service}.
   *
   * @throws Exception in case of any error while initializing
   */
  protected abstract void initialize() throws Exception;

  /**
   * Children classes should implement this method to add logic to the shutdown of this {@link Service}.
   *
   * @throws Exception in case of any error while shutting down
   */
  protected abstract void doShutdown() throws Exception;

  protected AbstractStreamService(StreamCoordinatorClient streamCoordinatorClient,
                                  StreamFileJanitorService janitorService) {
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.janitorService = janitorService;
  }

  protected StreamCoordinatorClient getStreamCoordinatorClient() {
    return streamCoordinatorClient;
  }

  @Override
  protected final void startUp() throws Exception {
    streamCoordinatorClient.startAndWait();
    janitorService.startAndWait();
    initialize();
  }

  @Override
  protected final void shutDown() throws Exception {
    doShutdown();

    if (executor != null) {
      executor.shutdownNow();
    }

    janitorService.stopAndWait();
    streamCoordinatorClient.stopAndWait();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, Constants.Stream.HEARTBEAT_INTERVAL, TimeUnit.SECONDS);
  }

  @Override
  protected ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("heartbeats-scheduler"));
    return executor;
  }
}
