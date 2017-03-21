/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.api.batch;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.batch.Input;

/**
 * Context of a Batch Source.
 */
@Beta
public interface BatchSourceContext extends BatchContext {

  /**
   * Overrides the input configuration of this Batch job to the specified {@link Input}.
   *
   * @param input the input to be used
   */
  void setInput(Input input);

  /**
   * Indicates whether the pipeline is running in preview.
   *
   * @return a boolean value which indicates the pipeline is running in preview mode.
   */
  boolean isPreviewEnabled();
}
