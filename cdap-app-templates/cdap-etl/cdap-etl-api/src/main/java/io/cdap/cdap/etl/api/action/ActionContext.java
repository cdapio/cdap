/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.etl.api.action;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.etl.api.StageContext;

/**
 * Represents the context available to the action plugin during runtime.
 */
public interface ActionContext extends StageContext, Transactional, SecureStore, SecureStoreManager {

  /**
   * Returns settable pipeline arguments. These arguments are shared by all pipeline stages, so plugins should be
   * careful to prefix any arguments that should not be clobbered by other pipeline stages.
   *
   * @return settable pipeline arguments
   */
  @Override
  SettableArguments getArguments();

}
