/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

/**
 * Contains various potential states and short error strings for a {@link UserIdentityExtractionResponse}.
 */
public enum UserIdentityExtractionState {
  SUCCESS("success"),
  ERROR_MISSING_CREDENTIAL("missing_credential"),
  ERROR_INVALID_TOKEN("invalid_token"),
  ERROR_MISSING_IDENTITY("missing_identity");

  private final String shortString;

  UserIdentityExtractionState(String shortString) {
    this.shortString = shortString;
  }

  public String toString() {
    return shortString;
  }
}
