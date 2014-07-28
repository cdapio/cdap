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

package com.continuuity.test.internal;

import com.continuuity.proto.Id;

/**
 * Default Ids to use in test if you do not want to construct your own.
 */
public class DefaultId {
  public static final String DEFAULT_ACCOUNT_ID = "developer"; // changed from default
  private static final String DEFAULT_APPLICATION_ID = "myapp";
  private static final String DEFAULT_PROGRAM_ID = "pgm";

  public static final Id.Account ACCOUNT = new Id.Account(DEFAULT_ACCOUNT_ID);
  public static final Id.Application APPLICATION = new Id.Application(ACCOUNT, DEFAULT_APPLICATION_ID);
  public static final Id.Program PROGRAM = new Id.Program(APPLICATION, DEFAULT_PROGRAM_ID);
}
