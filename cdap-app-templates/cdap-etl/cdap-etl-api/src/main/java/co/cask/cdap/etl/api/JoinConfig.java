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

  private String joinType;
  private int numOfInputs;
  private Iterable<String> requiredInputs;

  public JoinConfig(String joinType, int numOfInputs, Iterable<String> requiredInputs) {
    this.joinType = joinType;
    this.numOfInputs = numOfInputs;
    this.requiredInputs = requiredInputs;
  }

  public String getJoinType() {
    return joinType;
  }

  public int getNumOfInputs() {
    return numOfInputs;
  }

  public Iterable<String> getRequiredInputs() {
    return requiredInputs;
  }
}
