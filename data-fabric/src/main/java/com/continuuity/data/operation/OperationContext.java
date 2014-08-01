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

package com.continuuity.data.operation;

/**
 * Defines Operation Context.
 */
public class OperationContext {

  private String application;

  private String account;

  /**
   * Constructor for operation context.
   * @param account  account Id
   * @param application application id
   */
  public OperationContext(String account, String application) {

    if (account == null) {
      throw new IllegalArgumentException("account cannot be null");
    }
    if (account.isEmpty()) {
      throw new IllegalArgumentException("account cannot be empty");
    }
    if (application != null && application.isEmpty()) {
      throw new IllegalArgumentException("application cannot be empty");
    }
    this.account = account;
    this.application = application;
  }

  /**
   * Constructor for operation context.
   * @param account  account Id
   */
  public OperationContext(String account) {
    this(account, null);
  }

  /**
   * @return String application id
   */
  public String getApplication() {
    return this.application;
  }

  /**
   * @return String account Id
   */
  public String getAccount() {
    return account;
  }
}
