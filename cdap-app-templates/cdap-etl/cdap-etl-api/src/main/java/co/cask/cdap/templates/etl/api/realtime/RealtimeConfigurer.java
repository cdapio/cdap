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

package co.cask.cdap.templates.etl.api.realtime;

import co.cask.cdap.api.Resources;
import co.cask.cdap.templates.etl.api.StageConfigurer;

/**
 * Configurer for the Source/Sink stage of the Realtime Pipeline.
 */
public interface RealtimeConfigurer extends StageConfigurer {

  /**
   * Overrides the resources, such as memory and virtual cores, to use for the execution.
   *
   * @param resources Resources to be used for execution
   */
  void setResources(Resources resources);

  /**
   * Source/Sink can limit the scaling of instances.
   *
   * @param count max instance count
   */
  void setMaxInstances(int count);
}
