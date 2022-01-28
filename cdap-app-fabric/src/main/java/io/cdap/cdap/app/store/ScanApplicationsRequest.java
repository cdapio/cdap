/*
 * Copyright © 2022 Cask Data, Inc.
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

import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.SortOrder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Defines parameters of application scan within a store
 */
public class ScanApplicationsRequest {
  @Nullable
  private final NamespaceId namespaceId;
  @Nullable
  private final ApplicationId scanFrom;
  @Nullable
  private final ApplicationId scanTo;
  private final List<ApplicationFilter> filters;
  private final SortOrder sortOrder;

  /**
   *
   * @param namespaceId  namespace to return applications for or null for all namespaces
   * @param scanFrom     application id to start scan from (exclusive)
   * @param scanTo       application id to stop scan at (exclusive)
   * @param filters      additional filters to apply
   * @param sortOrder    sort order of the results
   */
  private ScanApplicationsRequest(@Nullable NamespaceId namespaceId,
                                  @Nullable ApplicationId scanFrom,
                                  @Nullable ApplicationId scanTo,
                                  List<ApplicationFilter> filters,
                                  SortOrder sortOrder) {
    this.namespaceId = namespaceId;
    this.scanFrom = scanFrom;
    this.scanTo = scanTo;
    this.filters = filters;
    this.sortOrder = sortOrder;
  }

  /**
   *
   * @return namespace to return applications for or null for all namespaces
   */
  @Nullable
  public NamespaceId getNamespaceId() {
    return namespaceId;
  }

  /**
   *
   * @return application id to start scan from (exclusive)
   */
  @Nullable
  public ApplicationId getScanFrom() {
    return scanFrom;
  }

  /**
   *
   * @return application id to stop scan at (exclusive)
   */
  @Nullable
  public ApplicationId getScanTo() {
    return scanTo;
  }

  /**
   *
   * @return additional filters to apply. All filters must be satisfied (and operation). For performance reasons
   * it's better to put {@link ApplicationFilter.ArtifactIdFilter} first.
   */
  public List<ApplicationFilter> getFilters() {
    return filters;
  }

  /**
   *
   * @return sort order of the results. Results are sorted by namespace, then application id in the Ascending
   * or Descending order.
   */
  public SortOrder getSortOrder() {
    return sortOrder;
  }

  @Override
  public String toString() {
    return "ScanApplicationsRequest{" +
      "namespaceId=" + namespaceId +
      "scanFrom=" + scanFrom +
      "scanTo=" + scanTo +
      "filters=" + filters +
      "sortOrder=" + sortOrder +
      '}';
  }

  /**
   *
   * @return builder to create a new {@link ScanApplicationsRequest}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   *
   * @param request original request to use as a template
   * @return builder to create a new {@link ScanApplicationsRequest} prefilled with passed in request values
   */
  public static Builder builder(ScanApplicationsRequest request) {
    return new Builder(request);
  }

  public static class Builder {
    @Nullable
    private NamespaceId namespaceId;
    @Nullable
    private ApplicationId scanFrom;
    @Nullable
    private ApplicationId scanTo;
    private List<ApplicationFilter> filters = new ArrayList<>();
    private SortOrder sortOrder = SortOrder.ASC;

    private Builder() {
    }

    private Builder(ScanApplicationsRequest request) {
      this.namespaceId = request.namespaceId;
      this.scanFrom = request.scanFrom;
      this.scanTo = request.scanTo;
      this.filters = request.filters;
      this.sortOrder = request.sortOrder;
    }

    /**
     * @param namespaceId namespace to scan in
     */
    public Builder setNamespaceId(NamespaceId namespaceId) {
      this.namespaceId = namespaceId;
      return this;
    }

    /**
     *
     * @param scanFrom restart the scan after specific application id. Useful for pagination. If namespace id
     *                 is set, application id must be within same namespace.
     */
    public Builder setScanFrom(ApplicationId scanFrom) {
      this.scanFrom = scanFrom;
      return this;
    }

    /**
     *
     * @param scanTo stop the scan before specific application id. If namespace id
     *                 is set, application id must be within same namespace.
     */
    public Builder setScanTo(ApplicationId scanTo) {
      this.scanTo = scanTo;
      return this;
    }

    /**
     *
     * @param filter adds a filter
     */
    public Builder addFilter(ApplicationFilter filter) {
      this.filters.add(filter);
      return this;
    }

    /**
     *
     * @param filters adds multiple filters
     */
    public Builder addFilters(Collection<ApplicationFilter> filters) {
      this.filters.addAll(filters);
      return this;
    }

    /**
     *
     * @param sortOrder scan order
     */
    public Builder setSortOrder(SortOrder sortOrder) {
      this.sortOrder = sortOrder;
      return this;
    }

    /**
     *
     * @return new {@link ScanApplicationsRequest}
     */
    public ScanApplicationsRequest build() {
      return new ScanApplicationsRequest(namespaceId, scanFrom, scanTo, filters, sortOrder);
    }
  }
}
