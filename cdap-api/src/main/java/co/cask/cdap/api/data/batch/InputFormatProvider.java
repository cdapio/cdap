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
 * Interface to be implemented by datasets used as input to a MapReduce.
 */
public interface InputFormatProvider {

  /**
   * @return the class name of the input format to use.
   */
  String getInputFormatClassName();

  /**
   * @return the configuration properties that the input format expects to find in the Hadoop configuration.
   */
  Map<String, String> getInputFormatConfiguration();

}
