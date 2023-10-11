/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operation;

import javax.annotation.Nullable;

/**
 * Defines parameters of operation run scan.
 */
public class ScanOperationRunsRequest {

  @Nullable
  private final String namespace;
  @Nullable
  private final String scanAfterRunId;
  private final int limit;
  private final OperationRunFilter filter;

  /**
   * Constructor for ScanOperationRunsRequest.
   *
   * @param namespace namespace to return runs for
   * @param scanAfterRunId run id to start scan from (exclusive)
   * @param filter additional filters to apply
   * @param limit maximum number of records to return
   */
  private ScanOperationRunsRequest(@Nullable String namespace, @Nullable String scanAfterRunId, int limit,
      OperationRunFilter filter) {
    this.namespace = namespace;
    this.scanAfterRunId = scanAfterRunId;
    this.limit = limit;
    this.filter = filter;
  }

  /**
   * namespace to return applications for.
   */

  @Nullable
  public String getNamespace() {
    return namespace;
  }

  /**
   * run id to start scan from (exclusive).
   */
  @Nullable
  public String getScanAfter() {
    return scanAfterRunId;
  }

  /**
   * additional filters to apply. All filters must be satisfied (and operation).
   */
  public OperationRunFilter getFilter() {
    return filter;
  }

  /**
   * maximum number of records to read.
   */
  public int getLimit() {
    return limit;
  }

  /**
   * Builder to create a new {@link ScanOperationRunsRequest}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder to create a new {@link ScanOperationRunsRequest} prefilled with passed in request
   * values.
   */
  public static Builder builder(ScanOperationRunsRequest request) {
    return new Builder(request);
  }

  /**
   * Builder for {@link ScanOperationRunsRequest}.
   */
  public static class Builder {

    @Nullable
    private String namespace;
    @Nullable
    private String scanAfterRunId;
    @Nullable
    private OperationRunFilter filter;
    private int limit = Integer.MAX_VALUE;

    private Builder() {
    }

    private Builder(ScanOperationRunsRequest request) {
      this.namespace = request.namespace;
      this.scanAfterRunId = request.scanAfterRunId;
      this.filter = request.filter;
      this.limit = request.limit;
    }

    /**
     * namespaceId namespace to scan in.
     */
    public Builder setNamespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * restart the scan after specific run id. Useful for pagination.
     */
    public Builder setScanAfter(String scanFromRunId) {
      this.scanAfterRunId = scanFromRunId;
      return this;
    }

    /**
     * filters to apply.
     */
    public Builder setFilter(
        OperationRunFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * limit maximum number of records to scan.
     */
    public Builder setLimit(int limit) {
      this.limit = limit;
      return this;
    }

    /**
     * return new {@link ScanOperationRunsRequest}.
     */
    public ScanOperationRunsRequest build() {
      if (filter == null) {
        filter = OperationRunFilter.emptyFilter();
      }
      return new ScanOperationRunsRequest(namespace, scanAfterRunId, limit, filter);
    }
  }
}
