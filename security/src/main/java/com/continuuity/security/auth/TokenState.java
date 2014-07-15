/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.security.auth;

/**
 * Different states attained after validating the token
 * <ul>
 *   <li>MISSING - the access token is missing in the request</li>
 *   <li>INVALID - the token digest did not match the expected value</li>
 *   <li>EXPIRED - the token is past the expiration timestamp</li>
 *   <li>INTERNAL - another error occurred in processing (represented by the exception "cause")</li>
 *   <li>VALID - the token is valid</li>
 * </ul>
 */
public enum TokenState {
  MISSING("Token is missing.", false),
  INVALID("Invalid token signature.", false),
  EXPIRED("Expired token.", false),
  INTERNAL("Invalid key for token.", false),
  VALID("Token is valid.", true);

  private final String msg;
  private final boolean valid;

  TokenState(String msg, boolean valid) {
    this.msg = msg;
    this.valid = valid;
  }

  /**
   *
   * @return the message associated with this token state describing the cause to be in this state
   */
  public String getMsg() {
    return msg;
  }

  /**
   *
   * @return {@code true} if this token state is valid, {@code false} otherwise
   */
  public boolean isValid() {
    return valid;
  }

  @Override
  public String toString() {
    return this.msg;
  }
}
