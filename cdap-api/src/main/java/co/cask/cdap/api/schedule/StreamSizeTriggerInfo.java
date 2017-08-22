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

package co.cask.cdap.api.schedule;

/**
 * The stream size trigger information to be passed to the triggered program.
 */
public interface StreamSizeTriggerInfo extends TriggerInfo {
  /**
   * @return The namespace of the stream.
   */
  String getStreamNamespace();

  /**
   * @return The name of the stream.
   */
  String getStreamName();

  /**
   * @return The size of data, in MB, that the stream has to receive to trigger the schedule.
   */
  int getTriggerMB();

  /**
   * Returns the logical start time of the triggered program. Logical start time is when the schedule decides to launch
   * the program when the stream size trigger is satisfied, i.e. when {@link #getStreamSize()} is at least
   * {@link #getTriggerMB()} larger than the {@link #getBaseStreamSize()}.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC)
   */
  long getLogicalStartTime();

  /**
   * @return Stream size in bytes at the moment when the stream size trigger is satisfied, i.e. the stream size is
   *         at least {@link #getTriggerMB()} larger than the {@link #getBaseStreamSize()}.
   */
  long getStreamSize();

  /**
   * @return Time in milliseconds of the previous stream size polling.
   */
  long getBasePollingTime();

  /**
   * @return Stream size in bytes from previous polling.
   */
  long getBaseStreamSize();
}
