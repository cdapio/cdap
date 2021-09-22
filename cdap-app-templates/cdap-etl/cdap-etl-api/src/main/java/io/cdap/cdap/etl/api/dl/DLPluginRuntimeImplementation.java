/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.dl;

import io.cdap.cdap.etl.api.StageLifecycle;
import io.cdap.cdap.etl.api.SubmitterLifecycle;

public interface DLPluginRuntimeImplementation extends StageLifecycle<DLPluginContext> {
  void transform(DLPluginContext context, DLArguments arguments);

  @Override
  default void initialize(DLPluginContext context) throws Exception {
    //No-op
  }

  @Override
  default void destroy() {
    //No-op
  }
}
