/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import io.cdap.cdap.api.app.AppStateStore;

/**
 * Provides implementation for {@link AppStateStore}.
 */
public interface AppStateStoreProvider {

  /**
   * Returns implementation for {@link AppStateStore} for this app
   *
   * @param namespace Namespace for the app
   * @param applicationName Application name
   * @return AppStateStore implementation
   */
  AppStateStore getStateStore(String namespace, String applicationName);
}
