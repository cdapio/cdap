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

package co.cask.cdap.runtime.spi.provisioner;

/**
 * An provision exception that indicates a failure that may succeed after a retry.
 */
public class RetryableProvisionException extends Exception {

  public RetryableProvisionException(String message) {
    super(message);
  }

  public RetryableProvisionException(Throwable cause) {
    super(cause);
  }

  public RetryableProvisionException(String message, Throwable cause) {
    super(message, cause);
  }
}
