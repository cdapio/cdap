/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.app.store;

import io.cdap.cdap.proto.sourcecontrol.SortBy;
import io.cdap.cdap.spi.data.SortOrder;

/**
 * Represents a request to scan source control metadata.
 */
public class ScanSourceControlMetadataRequest {

  private final String namespace;
  private final SortOrder sortOrder;
  private final SortBy sortOn;
  private final String scanAfter;
  private final int limit;
  private final SourceControlMetadataFilter filter;


  /**
   * Constructs a new ScanSourceControlMetadataRequest.
   *
   * @param namespace The namespace to scan for metadata.
   * @param sortOrder The sorting order for the results.
   * @param sortOn    The field to sort the results on.
   * @param scanAfter The cursor for scanning after a certain point.
   * @param limit     The maximum number of results to return.
   * @param filter    The filter criteria for metadata.
   */
  public ScanSourceControlMetadataRequest(String namespace, SortOrder sortOrder,
      SortBy sortOn, String scanAfter, int limit, SourceControlMetadataFilter filter) {
    this.namespace = namespace;
    this.sortOrder = sortOrder;
    this.sortOn = sortOn;
    this.scanAfter = scanAfter;
    this.limit = limit;
    this.filter = filter;
  }

  public SortOrder getSortOrder() {
    return sortOrder;
  }

  public SortBy getSortOn() {
    return sortOn;
  }

  public String getNamespace() {
    return namespace;
  }

  public int getLimit() {
    return limit;
  }

  public String getScanAfter() {
    return scanAfter;
  }

  public SourceControlMetadataFilter getFilter() {
    return filter;
  }

  public static ScanSourceControlMetadataRequest.Builder builder() {
    return new ScanSourceControlMetadataRequest.Builder();
  }

  public static ScanSourceControlMetadataRequest.Builder builder(
      ScanSourceControlMetadataRequest request) {
    return new ScanSourceControlMetadataRequest.Builder(request);
  }

  /**
   * Builder pattern for constructing instances of {@link ScanSourceControlMetadataRequest}.
   */
  public static class Builder {

    private String namespace;
    private SortOrder sortOrder = SortOrder.ASC;
    private String scanAfter;
    private SortBy sortOn;
    private int limit = Integer.MAX_VALUE;
    private SourceControlMetadataFilter filter;

    private Builder() {
    }

    private Builder(ScanSourceControlMetadataRequest request) {
      this.namespace = request.namespace;
      this.sortOn = request.sortOn;
      this.sortOrder = request.sortOrder;
      this.scanAfter = request.scanAfter;
      this.limit = request.limit;
      this.filter = request.filter;
    }

    public Builder setNamespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder setSortOrder(SortOrder sortOrder) {
      this.sortOrder = sortOrder;
      return this;
    }

    public Builder setSortOn(SortBy sortOn) {
      this.sortOn = sortOn;
      return this;
    }

    public Builder setScanAfter(String scanAfter) {
      this.scanAfter = scanAfter;
      return this;
    }

    public Builder setLimit(int limit) {
      this.limit = limit;
      return this;
    }

    public Builder setFilter(SourceControlMetadataFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Builds an instance of {@link ScanSourceControlMetadataRequest} based on the provided
     * parameters. If no filter is specified, an empty filter is applied.
     *
     * @return An instance of {@link ScanSourceControlMetadataRequest} configured with the builder's
     *         parameters.
     */
    public ScanSourceControlMetadataRequest build() {
      if (filter == null) {
        filter = SourceControlMetadataFilter.emptyFilter();
      }
      return new ScanSourceControlMetadataRequest(namespace, sortOrder, sortOn, scanAfter, limit,
          filter);
    }
  }
}
