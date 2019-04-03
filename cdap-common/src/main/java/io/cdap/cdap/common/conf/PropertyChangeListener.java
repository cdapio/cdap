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
package co.cask.cdap.common.conf;

/**
 * A listener for watching changes in {@link PropertyStore}.
 *
 * @param <T> Type of property
 */
public interface PropertyChangeListener<T> {

  /**
   * Invoked when the property changed.
   *
   * @param name Name of the property
   * @param property The updated property or {@code null} if the property is deleted.
   */
  void onChange(String name, T property);

  /**
   * Invoked when there is failure when listening for changes.
   *
   * @param failureCause The cause of the failure
   */
  void onError(String name, Throwable failureCause);
}
