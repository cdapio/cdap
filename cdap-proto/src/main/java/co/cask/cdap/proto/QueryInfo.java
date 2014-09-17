/*
 * Copyright © 2014 Cask Data, Inc.
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

import com.google.common.primitives.Longs;
import com.google.gson.annotations.SerializedName;

/**
 * Information about the query.
 */
public class QueryInfo implements Comparable<QueryInfo> {

  private final long timestamp;
  private final String statement;
  private final QueryStatus.OpStatus status;

  @SerializedName("query_handle")
  private final String queryHandle;

  @SerializedName("has_results")
  private final boolean hasResults;

  @SerializedName("is_active")
  private final boolean isActive;

  public QueryInfo(long timestamp, String query, QueryHandle handle,
                   QueryStatus status, boolean isActive) {
    this.timestamp = timestamp;
    this.statement = query;
    this.queryHandle = handle.getHandle();
    this.status = status.getStatus();
    this.hasResults = status.hasResults();
    this.isActive = isActive;
  }

  public String getStatement() {
    return statement;
  }

  public QueryStatus.OpStatus getStatus() {
    return status;
  }

  public String getQueryHandle() {
    return queryHandle;
  }

  public boolean isHasResults() {
    return hasResults;
  }

  public boolean isActive() {
    return isActive;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public int compareTo(QueryInfo queryInfo) {
    // descending.
    return Longs.compare(queryInfo.getTimestamp(), getTimestamp());
  }
}
