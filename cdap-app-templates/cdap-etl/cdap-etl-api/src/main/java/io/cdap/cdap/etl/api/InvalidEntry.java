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

import java.io.Serializable;

/**
 * Represents a record that fails validation, with provided errorCode and errorMessage
 *
 * @param <T> Type of the object that failed validation.
 */
public class InvalidEntry<T> implements Serializable {
  private static final long serialVersionUID = -1232949057204279740L;
  private final int errorCode;
  private final String errorMsg;
  private final T invalidRecord;

  public InvalidEntry(int errorCode, String errorMsg, T invalidRecord) {
    this.errorCode = errorCode;
    // since user code creates this object, just make sure it can't be null
    this.errorMsg = errorMsg == null ? "" : errorMsg;
    this.invalidRecord = invalidRecord;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getErrorMsg() {
    return errorMsg;
  }

  public T getInvalidRecord() {
    return invalidRecord;
  }
}
