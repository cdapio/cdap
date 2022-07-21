/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.app.deploy;

import io.cdap.cdap.internal.app.deploy.pipeline.AppSpecInfo;

import javax.annotation.Nullable;

/**
 * Interface defining the response as returned by the execution of the {@link Configurator#config()} method.
 */
public interface ConfigResponse {

  /**
   * Returns the {@link AppSpecInfo} or {@code null} if there is no response available.
   */
  @Nullable
  AppSpecInfo getAppSpecInfo();

  /**
   * @return 0 if succeeded
   */
  int getExitCode();
}
