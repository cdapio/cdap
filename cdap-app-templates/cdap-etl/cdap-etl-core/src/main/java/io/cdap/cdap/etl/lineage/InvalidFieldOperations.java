/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.etl.lineage;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * For a single stage, this class stores the invalid fields along with the
 * name of operations which are either using these fields as input or creating
 * these fields as output. Please see {@link StageOperationsValidator} for
 * checks that are done on the field before marking it as invalid.
 */
public class InvalidFieldOperations {
  // Map of invalid field names to the list of operations which are using them as input
  private final Map<String, List<String>> invalidInputs;

  // Map of invalid field names to the list of operations which created them as output
  private final Map<String, List<String>> invalidOutputs;

  public InvalidFieldOperations(Map<String, List<String>> invalidInputs, Map<String, List<String>> invalidOutputs) {
    this.invalidInputs = invalidInputs;
    this.invalidOutputs = invalidOutputs;
  }

  /**
   * @return map of invalid field names to the list of operations which are using them as input
   */
  public Map<String, List<String>> getInvalidInputs() {
    return invalidInputs;
  }

  /**
   * @return map of invalid field names to the list of operations which created them as output
   */
  public Map<String, List<String>> getInvalidOutputs() {
    return invalidOutputs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InvalidFieldOperations that = (InvalidFieldOperations) o;
    return Objects.equals(invalidInputs, that.invalidInputs) &&
      Objects.equals(invalidOutputs, that.invalidOutputs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(invalidInputs, invalidOutputs);
  }
}
