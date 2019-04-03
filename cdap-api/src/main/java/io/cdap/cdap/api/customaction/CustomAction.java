/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.api.customaction;

import co.cask.cdap.api.ProgramLifecycle;

/**
 * Defines custom action in the Workflow.
 */
public interface CustomAction extends ProgramLifecycle<CustomActionContext> {

  /**
   * Configure the custom action.
   * @param configurer the {@link CustomActionConfigurer} used to configure the action.
   */
  void configure(CustomActionConfigurer configurer);

  /**
   * Implementation should contain the code that will be executed by Workflow at runtime.
   * @throws Exception if there is any error executing the code
   */
  void run() throws Exception;
}
