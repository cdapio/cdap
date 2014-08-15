/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.security.zookeeper;

/**
 * Allows a client to receive notifications when the resources managed by {@link SharedResourceCache}
 * are updated.
 * @param <T> The resource type being managed by {@code SharedResourceCache}.
 */
public interface ResourceListener<T> {
  /**
   * Invoked when the entire set of cached resources has changed.
   */
  void onUpdate();

  /**
   * Invoked on an update to an individual resource.
   * @param name the key for the resource being updated
   * @param instance the resource instance which was updated
   */
  void onResourceUpdate(String name, T instance);

  /**
   * Invoked when a resource is removed from the shared cache.
   * @param name the key for the resource that has been removed
   */
  void onResourceDelete(String name);

  /**
   * Invoked when an error occurs in one of the resource operations.
   * @param name the key for the resource on which the error occurred
   * @param throwable the exception that was thrown
   */
  void onError(String name, Throwable throwable);
}
