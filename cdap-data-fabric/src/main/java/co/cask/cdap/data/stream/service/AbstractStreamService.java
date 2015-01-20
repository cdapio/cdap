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
 *
 */
public abstract class AbstractStreamService extends AbstractIdleService implements Service {

  private final StreamCoordinatorClient streamCoordinatorClient;
  private final StreamFileJanitorService janitorService;
  private final StreamWriterSizeManager sizeManager;

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
  }

  @Override
  protected final void shutDown() throws Exception {
    sizeManager.stopAndWait();
    janitorService.stopAndWait();
    streamCoordinatorClient.stopAndWait();
  }
}
