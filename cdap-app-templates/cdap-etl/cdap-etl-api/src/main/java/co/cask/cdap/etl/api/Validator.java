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

package co.cask.cdap.etl.api;

/**
 * Utility for obtaining an object which implements a set of validator functions; this object
 * is usually assigned to a variable specified by the
 * {@link co.cask.cdap.etl.api.Validator#getValidatorName} value.
 */
public interface Validator {

  /**
   * Name used as variable name for the Object returned by {@link co.cask.cdap.etl.api.Validator#getValidator}.
   */
  String getValidatorName();

  /**
   * Gets the Validator Object, on which the validator function's can be invoked from user-code.
   *
   * @return the validator object
   */
  Object getValidator();
}
