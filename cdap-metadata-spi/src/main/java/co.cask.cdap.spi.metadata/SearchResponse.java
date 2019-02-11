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

package co.cask.cdap.spi.metadata;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A response for a search request.
 */
public class SearchResponse {

  private final SearchRequest request;
  private final String cursor;
  private final int totalResults;
  private final List<MetadataRecord> results;

  /**
   * @param request the original request
   * @param cursor the cursor for the next search, if requested
   * @param totalResults the total number of results, or an estimate thereof
   * @param results the search results
   */
  public SearchResponse(SearchRequest request,
                        @Nullable String cursor,
                        int totalResults,
                        List<MetadataRecord> results) {
    this.request = request;
    this.cursor = cursor;
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
   * @return the cursor for the next page of results, if requested
   */
  // TODO (CDAP-14799) clearly explain the semantics of cursors, offsets, and total count
  public String getCursor() {
    return cursor;
  }

  /**
   * @return the estimated total number of results
   */
  // TODO (CDAP-14799) clearly explain the semantics of cursors, offsets, and total count
  @Nullable
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
    SearchResponse that = (SearchResponse) o;
    return totalResults == that.totalResults &&
      Objects.equals(request, that.request) &&
      Objects.equals(cursor, that.cursor) &&
      Objects.equals(results, that.results);
  }

  @Override
  public int hashCode() {
    return Objects.hash(request, cursor, totalResults, results);
  }
}
