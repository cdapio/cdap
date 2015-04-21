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

package co.cask.cdap.test;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

import java.io.IOException;
import java.util.List;

/**
 * This interface allows you to interact with streams.
 * Currently, it only exposes methods to read from a stream.
 * Eventually, {@link StreamWriter} should be deprecated and all its functionality moved here
 */
@Beta
public interface StreamManager {

  /**
   * Create the stream.
   *
   * @throws java.io.IOException If there is an error creating the stream.
   */
  void createStream() throws IOException;

  /**
   * Get events from the specified stream in the specified interval
   *
   * @param startTime the start time
   * @param endTime the end time
   * @param limit the maximum number of events to return
   * @return a list of stream events in the given time range
   */
  List<StreamEvent> getEvents(long startTime, long endTime, int limit) throws IOException;
}
