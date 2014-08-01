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

package com.continuuity.proto;

import com.google.common.base.Objects;

/**
 * Represents the status of an operation submitted to {@link Explore}.
 */
public class QueryStatus {

  public static final QueryStatus NO_OP = new QueryStatus(OpStatus.FINISHED, false);

  private final OpStatus status;
  private final boolean hasResults;

  public QueryStatus(OpStatus status, boolean hasResults) {
    this.status = status;
    this.hasResults = hasResults;
  }

  public OpStatus getStatus() {
    return status;
  }

  public boolean hasResults() {
    return hasResults;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("state", status)
      .add("hasResults", hasResults)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QueryStatus that = (QueryStatus) o;

    return Objects.equal(this.status, that.status) &&
      Objects.equal(this.hasResults, that.hasResults);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(NO_OP, status, hasResults);
  }

  /**
   * Represents the status of an operation.
   */
  @SuppressWarnings("UnusedDeclaration")
  public enum OpStatus {
    INITIALIZED,
    RUNNING,
    FINISHED,
    CANCELED,
    CLOSED,
    ERROR,
    UNKNOWN,
    PENDING;

    public boolean isDone() {
      return this.equals(FINISHED) || this.equals(CANCELED) || this.equals(CLOSED) || this.equals(ERROR);
    }
  }
}
