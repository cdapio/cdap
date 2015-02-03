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

package co.cask.cdap.data2;

/**
 * Defines OperationException.
 */
@Deprecated
public class OperationException extends Exception {

  int statusCode;

  /**
   * Constructor for operation exception.
   * @param statusCode status code
   * @param message a descriptive message for the error
   * @param cause the original throwable that caused this error
   */
  public OperationException(int statusCode, String message, Throwable cause) {
    super(message, cause);
    this.statusCode = statusCode;
  }

  /**
   * Get the status code.
   * @return status code
   */
  public int getStatus() {
    return this.statusCode;
  }

  /**
   * Get the status message.
   * @return the status message
   */
  public String getStatusMessage() {
    return String.format("[%d] %s", this.statusCode, super.getMessage());
  }

  @Override
  public String getMessage() {
    return getStatusMessage();
  }
}
