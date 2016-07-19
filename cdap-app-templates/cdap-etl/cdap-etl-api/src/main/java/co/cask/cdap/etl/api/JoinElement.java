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

import java.io.Serializable;

/**
 * Join element to hold join record per stage
 * @param <INPUT_RECORD> type of input record from each stage
 */
public final class JoinElement<INPUT_RECORD> implements Serializable {

  private static final long serialVersionUID = 7274266812214859025L;

  private final String stageName;
  private final INPUT_RECORD inputRecord;

  public JoinElement(String stageName, INPUT_RECORD inputRecord) {
    this.stageName = stageName;
    this.inputRecord = inputRecord;
  }

  /**
   * Returns stage name to which input record belongs to
   * @return stage name for input record
   */
  public String getStageName() {
    return stageName;
  }

  /**
   * Returns input record which is part of join result
   * @return input record to be merged
   */
  public INPUT_RECORD getInputRecord() {
    return inputRecord;
  }
}
