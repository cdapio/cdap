/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.api.condition;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.etl.api.StageContext;
import co.cask.cdap.etl.api.action.SettableArguments;

import java.util.Map;

/**
 * Represents the context available to the condition plugin during runtime.
 */
public interface ConditionContext extends StageContext, Transactional, SecureStore, SecureStoreManager {

  /**
   * Get a {@link Map} of stage name to the {@link StageStatistics}. This map will only contain the stages
   * those were executed before the execution of the condition.
   * @return stage statistics associated with the stages that were executed before the condition
   */
  Map<String, StageStatistics> getStageStatistics();

  /**
   * Return the arguments which can be updated.
   */
  @Override
  SettableArguments getArguments();
}
