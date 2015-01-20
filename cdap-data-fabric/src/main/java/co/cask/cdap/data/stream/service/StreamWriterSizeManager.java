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

import com.google.common.util.concurrent.Service;

/**
 * This interface manages the sizes of data written by one stream writer. For each stream, It sends
 * {@link StreamWriterHeartbeat}s at regular intervals to notify listeners of the updated size of the stream.
 */
public interface StreamWriterSizeManager extends Service {

  /**
   * Get the initial sizes of data written by this Stream writer, and send an initial heartbeat with the computed size,
   * for each stream. This method also schedules publishing heartbeats at a regular pace.
   */
  void initialize();
}
