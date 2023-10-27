/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.sourcecontrol;


/**
 * Factory interface for creating {@link PushAppsOperation}.
 * This interface is for Guice assisted binding, hence there will be no concrete implementation of it.
 */
public interface PushAppsOperationFactory {

  /**
   * Returns an implementation of {@link PushAppsOperation} that operates on the given {@link
   * PushAppsRequest}.
   *
   * @param request contains list of apps to push
   * @return a new instance of {@link PushAppsOperation}.
   */
  PushAppsOperation create(PushAppsRequest request);
}
