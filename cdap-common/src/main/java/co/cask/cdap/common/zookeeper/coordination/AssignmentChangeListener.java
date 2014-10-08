/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.common.zookeeper.coordination;

/**
 * Listener to watch for changes in assignments.
 */
public interface AssignmentChangeListener {

  /**
   * Invoked when there is change in assignment.
   *
   * @param assignment The updated assignment.
   */
  void onChange(ResourceAssignment assignment);

  /**
   * Invoked when no more changes will be notified to this listener.
   *
   * @param failureCause Failure that causes notification stopped or {@code null} if the completion is not caused by
   *                     failure (e.g. upon request).
   */
  void finished(Throwable failureCause);
}
