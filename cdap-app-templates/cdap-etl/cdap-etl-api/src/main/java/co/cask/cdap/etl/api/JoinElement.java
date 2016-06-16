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
 * Join element to hold per stage join value
 * @param <JOIN_VALUE> join value per stage
 */
public class JoinElement<JOIN_VALUE> {

  private String stageName;
  private JOIN_VALUE joinValue;

  public JoinElement(String stageName, JOIN_VALUE joinValue) {
    this.stageName = stageName;
    this.joinValue = joinValue;
  }

  public String getStageName() {
    return stageName;
  }

  public void setStageName(String stageName) {
    this.stageName = stageName;
  }

  public JOIN_VALUE getJoinValue() {
    return joinValue;
  }

  public void setJoinValue(JOIN_VALUE joinValue) {
    this.joinValue = joinValue;
  }
}
