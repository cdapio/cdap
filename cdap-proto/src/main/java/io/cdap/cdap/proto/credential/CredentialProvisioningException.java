/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.credential;

/**
 * Exception thrown during credential provisioning.
 */
public class CredentialProvisioningException extends Exception {

  /**
   * Creates a new credential provisioning exception.
   *
   * @param message The message for the provisioning failure.
   */
  public CredentialProvisioningException(String message) {
    super(message);
  }

  /**
   * Creates a new credential provisioning exception.
   *
   * @param cause cause of the provisioning failure.
   */
  public CredentialProvisioningException(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a new credential provisioning exception.
   *
   * @param message The message for the provisioning failure.
   * @param cause cause of the failure.
   */
  public CredentialProvisioningException(String message, Throwable cause) {
    super(message, cause);
  }
}
