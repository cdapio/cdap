/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.explore.service;

import com.continuuity.proto.QueryStatus;

/**
 * Exception thrown in case a query execution ends in an unexpected state.
 */
public class UnexpectedQueryStatusException extends Exception {

  private final QueryStatus.OpStatus status;

  public UnexpectedQueryStatusException(String s, QueryStatus.OpStatus status) {
    super(s);
    this.status = status;
  }

  public QueryStatus.OpStatus getStatus() {
    return status;
  }
}
