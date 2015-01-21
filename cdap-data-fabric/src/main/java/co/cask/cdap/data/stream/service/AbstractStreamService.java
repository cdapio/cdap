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

import co.cask.cdap.data.stream.StreamCoordinatorClient;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;

/**
 * Stream service meant to run in an HTTP service.
 */
public abstract class AbstractStreamService extends AbstractIdleService implements StreamService {

  private final StreamCoordinatorClient streamCoordinatorClient;
  private final StreamFileJanitorService janitorService;
  private final StreamWriterSizeManager sizeManager;

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
                                  StreamFileJanitorService janitorService,
                                  StreamWriterSizeManager sizeManager) {
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.janitorService = janitorService;
    this.sizeManager = sizeManager;
  }

  @Override
  protected final void startUp() throws Exception {
    streamCoordinatorClient.startAndWait();
    janitorService.startAndWait();
    sizeManager.startAndWait();
    sizeManager.initialize();
    initialize();
  }

  @Override
  protected final void shutDown() throws Exception {
    doShutdown();
    sizeManager.stopAndWait();
    janitorService.stopAndWait();
    streamCoordinatorClient.stopAndWait();
  }
}
