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

package io.cdap.cdap.security.spi.authorization;

/**
 * A helper class to support the response for different method calls that returns an object but also needs to
 * get the {@link AuthorizationResponse} for the call.
 *
 * @param <T> : This is the return object of the method call
 */
public class AuthorizedResult<T> {

  private T result;
  private AuthorizationResponse authorizationResponse;

  public AuthorizedResult(T result, AuthorizationResponse authorizationResponse) {
    this.result = result;
    this.authorizationResponse = authorizationResponse;
  }

  public T getResult() {
    return result;
  }

  public AuthorizationResponse getAuthorizationResponse() {
    return authorizationResponse;
  }
}
