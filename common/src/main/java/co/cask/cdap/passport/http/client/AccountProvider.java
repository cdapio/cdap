/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.passport.http.client;

import co.cask.cdap.passport.meta.Account;

/**
 * Overriding accountId type to String. Ideally Account should use String type for accountId.
 * TODO: (ENG-2213) - Use String for accountId remove AccountProvider class                 .
 * @param <T> specific {@link Account} Type
 */
public class AccountProvider<T extends Account> {
  private final T t;

  /**
   * Construct account provider specific {@link Account} Type.
   * @param t Instance of {@link Account} Type
   */
  public AccountProvider(T t) {
    this.t = t;
  }

  /**
   * @return instance of {@link Account}
   */
  public T get() {
    return t;
  }

  /**
   * @return String representation of accountId
   */
  public String getAccountId() {
    return String.format("%s", t.getAccountId());
  }
}
