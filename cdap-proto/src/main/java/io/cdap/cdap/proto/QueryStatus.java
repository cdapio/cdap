/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.proto;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents the status of a submitted query operation.
 */
public class QueryStatus {

  public static final QueryStatus NO_OP = new QueryStatus(OpStatus.FINISHED, false);

  private final OpStatus status;
  private final boolean hasResults;
  private final String errorMessage;
  private String sqlState;

  /**
   * A query status for an operation that is not in error.
   * @param status the status
   * @param hasResults whether the query has produced results
   */
  public QueryStatus(OpStatus status, boolean hasResults) {
    this(status, hasResults, null, null);
  }

  /**
   * A query status that represents failure with an error message.
   * @param errorMessage the error message
   */
  public QueryStatus(String errorMessage, @Nullable String sqlState) {
    this(OpStatus.ERROR, false, errorMessage, sqlState);
  }

  private QueryStatus(OpStatus status, boolean hasResults, @Nullable String errorMessage, @Nullable String sqlState) {
    this.status = status;
    this.hasResults = hasResults;
    this.errorMessage = errorMessage;
    this.sqlState = sqlState;
  }

  public OpStatus getStatus() {
    return status;
  }

  public boolean hasResults() {
    return hasResults;
  }

  @Nullable
  public String getErrorMessage() {
    return errorMessage;
  }

  public String getSqlState() {
    return sqlState;
  }

  @Override
  public String toString() {
    return "QueryStatus{" +
      "status=" + status +
      ", hasResults=" + hasResults +
      ", errorMessage='" + errorMessage + '\'' +
      ", sqlState='" + sqlState + '\'' +
      '}';
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

    return Objects.equals(this.status, that.status) &&
      Objects.equals(this.hasResults, that.hasResults) &&
      Objects.equals(this.errorMessage, that.errorMessage) &&
      Objects.equals(this.sqlState, that.sqlState);
  }

  @Override
  public int hashCode() {
    return Objects.hash(NO_OP, status, hasResults, errorMessage, sqlState);
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
