/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.DatasetConfigurer;
import co.cask.cdap.api.ProgramConfigurer;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.plugin.PluginConfigurer;

/**
 * Configurer for configuring {@link MapReduce}.
 */
public interface MapReduceConfigurer extends DatasetConfigurer, ProgramConfigurer, PluginConfigurer {

  /**
   * Sets the name of the dataset used as input for the {@link MapReduce}.
   *
   * @deprecated as of 3.4.0. Use {@link MapReduceContext#addInput(Input)}
   *             in beforeSubmit, instead.
   */
  @Deprecated
  void setInputDataset(String dataset);

  /**
   * Sets the name of the dataset used as output for the {@link MapReduce}.
   *
   * @deprecated as of 3.4.0. Use {@link MapReduceContext#addOutput(Output)}
   *             in beforeSubmit, instead.
   */
  @Deprecated
  void setOutputDataset(String dataset);

  /**
   * Sets the resources requirement for the driver of the MapReduce.
   */
  void setDriverResources(Resources resources);

  /**
   * Sets the resources requirement for Mapper task of the {@link MapReduce}.
   */
  void setMapperResources(Resources resources);

  /**
   * Sets the resources requirement for Reducer task of the {@link MapReduce}.
   */
  void setReducerResources(Resources resources);
}
