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

package co.cask.cdap.data.stream;

import co.cask.cdap.proto.Id;

import java.util.Set;

/**
 * Defines a behavior that is triggered when a Stream handler becomes leader of a Stream, or a collection of streams.
 */
public interface StreamLeaderListener {

  /**
   * This method is called to specify that the Stream handler it is called from
   * is the leader of all {@code streamIds}.
   *
   * @param streamIds stream Ids of which the current Stream handler became leader
   */
  void leaderOf(Set<Id.Stream> streamIds);
}
