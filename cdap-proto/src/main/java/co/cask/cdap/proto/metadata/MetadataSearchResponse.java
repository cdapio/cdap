/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.proto.metadata;

import java.util.Set;

/**
 * Denotes the response of the metadata search API.
 */
public class MetadataSearchResponse {
  private final String sort;
  private final int offset;
  private final int size;
  private final int total;
  private final Set<MetadataSearchResultRecord> results;

  public MetadataSearchResponse(String sort, int offset, int size, int total, Set<MetadataSearchResultRecord> results) {
    this.sort = sort;
    this.offset = offset;
    this.size = size;
    this.total = total;
    this.results = results;
  }

  public String getSort() {
    return sort;
  }

  public int getOffset() {
    return offset;
  }

  public int getSize() {
    return size;
  }

  public int getTotal() {
    return total;
  }

  public Set<MetadataSearchResultRecord> getResults() {
    return results;
  }
}
