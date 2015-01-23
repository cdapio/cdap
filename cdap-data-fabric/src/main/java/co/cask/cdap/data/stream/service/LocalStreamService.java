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
import com.google.inject.Inject;

/**
 * Stream service running in local mode.
 */
public class LocalStreamService extends AbstractStreamService {

  @Inject
  public LocalStreamService(StreamCoordinatorClient streamCoordinatorClient,
                            StreamFileJanitorService janitorService) {
    super(streamCoordinatorClient, janitorService);
  }

  @Override
  protected void initialize() throws Exception {
    // No-op
  }

  @Override
  protected void doShutdown() throws Exception {
    // No-op
  }
}
