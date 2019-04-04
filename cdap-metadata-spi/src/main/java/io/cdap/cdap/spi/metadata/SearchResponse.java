/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.api.annotation.Beta;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A response for a search request.
 */
@Beta
public class SearchResponse {

  private final SearchRequest request;
  private final String cursor;
  private final int offset;
  private final int limit;
  private final int totalResults;
  private final List<MetadataRecord> results;

  /**
   * @param request the original request
   * @param cursor the cursor for the next search, if requested and there are more results
   * @param offset the offset at which the search results start
   * @param limit the limit that was applied to this search (the number of results can be less)
   * @param totalResults the total number of results, or an estimate thereof
   * @param results the search results
   */
  public SearchResponse(SearchRequest request,
                        @Nullable String cursor,
                        int offset, int limit, int totalResults,
                        List<MetadataRecord> results) {
    this.request = request;
    this.cursor = cursor;
    this.offset = offset;
    this.limit = limit;
    this.totalResults = totalResults;
    this.results = results;
  }

  /**
   * @return the original search request
   */
  public SearchRequest getRequest() {
    return request;
  }

  /**
   * @return the cursor for the next page of results, if requested, or null if there are no more results
   */
  @Nullable
  public String getCursor() {
    return cursor;
  }

  /**
   * @return the offset at which the results begin
   */
  public int getOffset() {
    return offset;
  }

  /**
   * @return the limit that was applied to the search (may be greater than the number of results)
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @return the estimated total number of results. If this is greater than {@link #getOffset()} plus
   * the size of {@link #getResults()}, then there are more results.
   */
  public int getTotalResults() {
    return totalResults;
  }

  /**
   * @return the search results
   */
  public List<MetadataRecord> getResults() {
    return results;
  }

  @Override
  public String toString() {
    return "SearchResponse{" +
      "request=" + request +
      ", cursor='" + cursor + '\'' +
      ", offset=" + offset +
      ", limit=" + limit +
      ", totalResults=" + totalResults +
      ", results=" + results +
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
    SearchResponse response = (SearchResponse) o;
    return offset == response.offset &&
      limit == response.limit &&
      totalResults == response.totalResults &&
      Objects.equals(request, response.request) &&
      Objects.equals(cursor, response.cursor) &&
      Objects.equals(results, response.results);
  }

  @Override
  public int hashCode() {
    return Objects.hash(request, cursor, offset, limit, totalResults, results);
  }
}
