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
import co.cask.cdap.data.stream.service.heartbeat.HeartbeatPublisher;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * {@link StreamWriterSizeManager} for in-memory Streams.
 */
public class InMemoryStreamWriterSizeManager extends AbstractStreamWriterSizeManager {

  @Inject
  public InMemoryStreamWriterSizeManager(HeartbeatPublisher heartbeatPublisher,
                                         @Named(Constants.Stream.CONTAINER_INSTANCE_ID) int instanceId) {
    super(heartbeatPublisher, instanceId);
  }

  @Override
  protected void init() throws Exception {
    // No-op
  }
}
