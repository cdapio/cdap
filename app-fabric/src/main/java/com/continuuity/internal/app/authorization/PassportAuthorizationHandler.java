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

package com.continuuity.internal.app.authorization;

import com.continuuity.app.authorization.AuthorizationHandler;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.proto.Id;

/**
 *
 */
public class PassportAuthorizationHandler implements AuthorizationHandler {
  private final CConfiguration configuration;

  public PassportAuthorizationHandler(CConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public boolean authroize(Id.Account account) {
    return false;
  }
}
