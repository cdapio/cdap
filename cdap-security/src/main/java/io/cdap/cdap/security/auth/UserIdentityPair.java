/*
 * Copyright © 2020-2021 Cask Data, Inc.
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

import javax.annotation.Nullable;

/**
 * Contains a {@link UserIdentity} backed by a user credential in String form.
 */
public class UserIdentityPair {
  private final String userCredential;
  private final UserIdentity userIdentityObj;

  public UserIdentityPair(String userCredential, UserIdentity userIdentityObj) {
    this.userIdentityObj = userIdentityObj;
    this.userCredential = userCredential;
  }

  /**
   * Returns the user credential representing the included {@link UserIdentity}.
   * The format of the credential may change depending on the {@link UserIdentityExtractor} implementation used.
   * @return the end-user credential
   */
  @Nullable
  public String getUserCredential() {
    return userCredential;
  }

  public UserIdentity getUserIdentity() {
    return userIdentityObj;
  }
}
