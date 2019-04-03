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

package co.cask.cdap.etl.api;

/**
 * Join configuration to hold information about join to be performed
 */
public class JoinConfig {
  private Iterable<String> requiredInputs;

  /**
   * Creates new instance of {@link JoinConfig}
   * @param requiredInputs iterable of input stage names. This will be used to find out type of the join.
   * If required inputs are empty, full outer join will be performed. Otherwise, all records from required inputs
   * will be joined using inner join and records from non-required inputs will be present in join result only if they
   * meet join criteria.
   */
  public JoinConfig(Iterable<String> requiredInputs) {
    this.requiredInputs = requiredInputs;
  }

  /**
   * Returns required inputs to be joined.
   * @return iterable of required inputs
   */
  public Iterable<String> getRequiredInputs() {
    return requiredInputs;
  }
}
