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

package co.cask.cdap.security.auth;

/**
 * Exception thrown if an asserted message digest does not match the recomputed value, using the same secret key.
 * This can occur if a message digest has been forged (without knowledge of the correct secret key), or if the
 * message contents have been tampered with.
 */
public class InvalidDigestException extends Exception {
  public InvalidDigestException(String message) {
    super(message);
  }
}
