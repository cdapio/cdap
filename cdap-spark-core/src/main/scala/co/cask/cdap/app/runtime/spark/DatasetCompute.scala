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

package co.cask.cdap.app.runtime.spark

import co.cask.cdap.api.dataset.Dataset

import scala.reflect.ClassTag

/**
  * A trait for [[co.cask.cdap.app.runtime.spark.DatasetRDD]] to acquire [[co.cask.cdap.api.dataset.Dataset]] instance
  * and performs computation on it.
  */
private[spark] trait DatasetCompute {

  /**
    * Performs computation on a [[co.cask.cdap.api.dataset.Dataset]] instance.
    *
    * @param datasetName name of the dataset
    * @param arguments arguments for the dataset
    * @param computeFunc function to operate on the dataset instance
    * @tparam T type of the result
    * @return result of the computation by the function
    */
  def apply[T: ClassTag](datasetName: String, arguments: Map[String, String], computeFunc: Dataset => T): T
}
