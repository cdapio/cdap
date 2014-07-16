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

package com.continuuity.common.zookeeper.election;

/**
 * Handles events of election/un-election of leader.
 */
public interface ElectionHandler {

  /**
   * This method will get invoked when a participant becomes a leader in a
   * leader election process. It is guaranteed that this method won't get called
   * consecutively (i.e. called twice or more in a row).
   */
  void leader();

  /**
   * This method will get invoked when a participant is a follower in a
   * leader election process. This method might get called multiple times without
   * the {@link #leader()} method being called.
   */
  void follower();
}
