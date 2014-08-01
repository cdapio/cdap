/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.logging.read;

/**
 * Callback to handle log events.
 */
public interface Callback {
  /**
   * Called once at the beginning before calling @{link handle}.
   */
  void init();

  /**
   * Called for every log event.
   * @param event log event.
   */
  void handle(LogEvent event);

  /**
   * Called once at the end after all log events are done.
   */
  void close();
}
