/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.k8s.common;

/**
 * Listener for listening to changes in K8s resources.
 *
 * @param <T> type of the resource
 */
public interface ResourceChangeListener<T> {

  /**
   * Invoked when a resource is added. By default is an no-op.
   *
   * @param resource the resource being added
   */
  default void resourceAdded(T resource) {
    // no-op
  }

  /**
   * Invoked when a resource is modified. By default is an no-op.
   *
   * @param resource the resource being modified
   */
  default void resourceModified(T resource) {
    // no-op
  }

  /**
   * Invoked when a resource is deleted. By default is an no-op.
   *
   * @param resource the resource being deleted
   */
  default void resourceDeleted(T resource) {
    // no-op
  }
}
