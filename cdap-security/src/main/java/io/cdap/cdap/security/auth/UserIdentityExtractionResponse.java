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
 * Contains the {@link UserIdentityExtractionState} and {@link UserIdentityPair} if extraction was successful or an
 * extended error message if the extraction failed.
 */
public class UserIdentityExtractionResponse {
  private final UserIdentityExtractionState state;
  private final UserIdentityPair identityPair;
  private final String errorDescription;

  public UserIdentityExtractionResponse(UserIdentityPair identityPair) {
    this(UserIdentityExtractionState.SUCCESS, identityPair, null);
  }

  public UserIdentityExtractionResponse(UserIdentityExtractionState state, String errorDescription) {
    this(state, null, errorDescription);
  }

  public UserIdentityExtractionResponse(UserIdentityExtractionState state, UserIdentityPair identityPair,
                                        String errorDescription) {
    this.state = state;
    this.identityPair = identityPair;
    this.errorDescription = errorDescription;
  }

  public boolean success() {
    return state.equals(UserIdentityExtractionState.SUCCESS);
  }

  public UserIdentityExtractionState getState() {
    return state;
  }

  public UserIdentityPair getIdentityPair() {
    return identityPair;
  }

  public String getErrorDescription() {
    return errorDescription;
  }
}
