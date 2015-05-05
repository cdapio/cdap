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

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.PropertyProvider;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * This class provides the specification for a MapReduce job.
 */
public interface MapReduceSpecification extends ProgramSpecification, PropertyProvider {

  /**
   * @return An immutable set of {@link co.cask.cdap.api.dataset.Dataset DataSets} that
   *         are used by the {@link MapReduce}.
   */
  Set<String> getDataSets();

  /**
   * @return name of the dataset to be used as output of mapreduce job or {@code null} if no dataset is used as output
   *         destination
   */
  @Nullable
  String getOutputDataSet();

  /**
   * @return name The name of the dataset to be used as input to a MapReduce job or {@code null} 
   * if no dataset is used as the input source.
   */
  @Nullable
  String getInputDataSet();

  /**
   * @return Resources requirement for mapper task or {@code null} if not specified.
   */
  @Nullable
  Resources getMapperResources();

  /**
   * @return Resources requirement for reducer task or {@code null} if not specified.
   */
  @Nullable
  Resources getReducerResources();
}
