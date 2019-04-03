/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.api.spark.dynamic;

import java.util.Arrays;

/**
 * Exception to indicate there is an error in binding variable.
 */
public class BindingException extends Exception {

  public BindingException(String name, String type, Object value, String...modifies) {
    super(String.format("Failed to bind variable %s with type %s to value %s%s.",
                        name, type, value, modifies.length == 0 ? "" : " with modifiers " + Arrays.asList(modifies)));
  }
}
