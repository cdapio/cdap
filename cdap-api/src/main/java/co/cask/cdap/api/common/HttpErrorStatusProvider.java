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

package co.cask.cdap.api.common;

/**
 * Interface to be used by all error related classes which needs to provide a status code.
 */
public interface HttpErrorStatusProvider {

  /**
   * Gives the HTTP status code for this error. All classes which needs to return a HTTP status code for an error
   * should override this.
   *
   * @return int which is the HTTP status code of this error
   */
  int getStatusCode();

  /**
   * Gives the error message.
   *
   * @return error message
   */
  String getMessage();
}
