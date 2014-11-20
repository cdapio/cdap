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

package co.cask.cdap.api.data.batch;

import java.util.Map;

/**
 * Interface to be implemented by datasets used as output of a MapReduce.
 */
public interface OutputFormatProvider {

  /**
   * @return the class of the output format to use.
   * @param <T> This should be the OutputFormat class. The type parameter is
   *           used here to avoid adding a dependency on Hadoop.
   */
  <T> Class<? extends T> getOutputFormatClass();

  /**
   * @return the configuration properties that the output format expects to
   *         find in the Hadoop configuration.
   */
  Map<String, String> getOutputFormatConfiguration();

}
