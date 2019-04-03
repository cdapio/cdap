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

package co.cask.cdap.api.mapreduce;

import co.cask.cdap.api.ProgramLifecycle;

/**
 * Defines an interface for the MapReduce job. Use it for easy integration (re-use) of existing MapReduce jobs
 * that rely on the Hadoop MapReduce APIs.
 */
public interface MapReduce {

  /**
   * Configures a {@link MapReduce} job using the given {@link MapReduceConfigurer}.
   */
  void configure(MapReduceConfigurer configurer);
}
