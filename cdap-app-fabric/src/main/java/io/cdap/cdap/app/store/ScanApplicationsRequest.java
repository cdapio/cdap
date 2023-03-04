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
import io.cdap.cdap.proto.id.ApplicationReference;
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
  private final String application;
  @Nullable
  private final ApplicationId scanFrom;
  @Nullable
  private final ApplicationId scanTo;
  private final List<ApplicationFilter> filters;
  private final SortOrder sortOrder;
  private final int limit;
  private final boolean latestOnly;
  private final boolean sortCreationTime;

  /**
   * @param namespaceId namespace to return applications for or null for all namespaces
   * @param application application to return applications for
   * @param scanFrom application id to start scan from (exclusive)
   * @param scanTo application id to stop scan at (exclusive)
   * @param filters additional filters to apply
   * @param sortOrder sort order of the results
   * @param limit maximum number of records to return
   */
  private ScanApplicationsRequest(@Nullable NamespaceId namespaceId,
      @Nullable String application,
      @Nullable ApplicationId scanFrom,
      @Nullable ApplicationId scanTo,
      List<ApplicationFilter> filters,
      SortOrder sortOrder, int limit,
      boolean latestOnly, boolean sortCreationTime) {
    this.namespaceId = namespaceId;
    this.application = application;
    this.scanFrom = scanFrom;
    this.scanTo = scanTo;
    this.filters = filters;
    this.sortOrder = sortOrder;
    this.limit = limit;
    this.latestOnly = latestOnly;
    this.sortCreationTime = sortCreationTime;
  }

  /**
   * @return namespace to return applications for or null for all namespaces
   */
  @Nullable
  public NamespaceId getNamespaceId() {
    return namespaceId;
  }

  /**
   * @return application to scan for
   */
  @Nullable
  public String getApplication() {
    return application;
  }

  /**
   * @return application id to start scan from (exclusive)
   */
  @Nullable
  public ApplicationId getScanFrom() {
    return scanFrom;
  }

  /**
   * @return application id to stop scan at (exclusive)
   */
  @Nullable
  public ApplicationId getScanTo() {
    return scanTo;
  }

  /**
   * @return additional filters to apply. All filters must be satisfied (and operation). For
   *     performance reasons it's better to put {@link ApplicationFilter.ArtifactIdFilter} first.
   */
  public List<ApplicationFilter> getFilters() {
    return filters;
  }

  /**
   * @return sort order of the results. Results are sorted by namespace, then application id in the
   *     Ascending or Descending order.
   */
  public SortOrder getSortOrder() {
    return sortOrder;
  }

  /**
   * @return maximum number of records to read
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @return whether to return the latest version of an application
   */
  public boolean getLatestOnly() {
    return latestOnly;
  }

  /**
   * @return a boolean to determine the application scan range field if true, range should use
   *     (namespace-app-creationTime) if false, range should use default (namespace-app-version)
   */
  public boolean getSortCreationTime() {
    return sortCreationTime;
  }

  @Override
  public String toString() {
    return "ScanApplicationsRequest{"
        + "namespaceId=" + namespaceId
        + ", application=" + application
        + ", scanFrom=" + scanFrom
        + ", scanTo=" + scanTo
        + ", filters=" + filters
        + ", sortOrder=" + sortOrder
        + ", limit=" + limit
        + ", latestOnly=" + latestOnly
        + ", sortCreationTime=" + sortCreationTime
        + '}';
  }

  /**
   * @return builder to create a new {@link ScanApplicationsRequest}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @param request original request to use as a template
   * @return builder to create a new {@link ScanApplicationsRequest} prefilled with passed in
   *     request values
   */
  public static Builder builder(ScanApplicationsRequest request) {
    return new Builder(request);
  }

  public static class Builder {

    @Nullable
    private NamespaceId namespaceId;
    @Nullable
    private String application;
    @Nullable
    private ApplicationId scanFrom;
    @Nullable
    private ApplicationId scanTo;
    private List<ApplicationFilter> filters = new ArrayList<>();
    private SortOrder sortOrder = SortOrder.ASC;
    private int limit = Integer.MAX_VALUE;
    private boolean latestOnly;
    private boolean sortCreationTime;

    private Builder() {
    }

    private Builder(ScanApplicationsRequest request) {
      this.namespaceId = request.namespaceId;
      this.application = request.application;
      this.scanFrom = request.scanFrom;
      this.scanTo = request.scanTo;
      this.filters = request.filters;
      this.sortOrder = request.sortOrder;
      this.limit = request.limit;
      this.latestOnly = request.latestOnly;
      this.sortCreationTime = request.sortCreationTime;
    }

    /**
     * @param namespaceId namespace to scan in
     */
    public Builder setNamespaceId(NamespaceId namespaceId) {
      this.namespaceId = namespaceId;
      return this;
    }

    /**
     * @param applicationReference application to scan in without version
     */
    public Builder setApplicationReference(ApplicationReference applicationReference) {
      this.namespaceId = applicationReference.getNamespaceId();
      this.application = applicationReference.getApplication();
      return this;
    }

    /**
     * @param scanFrom restart the scan after specific application id. Useful for pagination. If
     *     namespace id is set, application id must be within same namespace.
     */
    public Builder setScanFrom(ApplicationId scanFrom) {
      this.scanFrom = scanFrom;
      return this;
    }

    /**
     * @param scanTo stop the scan before specific application id. If namespace id is set,
     *     application id must be within same namespace.
     */
    public Builder setScanTo(ApplicationId scanTo) {
      this.scanTo = scanTo;
      return this;
    }

    /**
     * @param filter adds a filter
     */
    public Builder addFilter(ApplicationFilter filter) {
      this.filters.add(filter);
      return this;
    }

    /**
     * @param filters adds multiple filters
     */
    public Builder addFilters(Collection<ApplicationFilter> filters) {
      this.filters.addAll(filters);
      return this;
    }

    /**
     * @param sortOrder scan order
     */
    public Builder setSortOrder(SortOrder sortOrder) {
      this.sortOrder = sortOrder;
      return this;
    }

    /**
     * @param limit maximum number of records to scan
     */
    public Builder setLimit(int limit) {
      this.limit = limit;
      return this;
    }

    /**
     * @param latestOnly whether to return the latest version of an application
     */
    public Builder setLatestOnly(boolean latestOnly) {
      this.latestOnly = latestOnly;
      return this;
    }

    /**
     * @param sortCreationTime a boolean to determine the application scan range field if true,
     *     range should use (namespace-app-creationTime) if false, range should use default
     *     (namespace-app-version)
     */
    public Builder setSortCreationTime(boolean sortCreationTime) {
      this.sortCreationTime = sortCreationTime;
      return this;
    }

    /**
     * @return new {@link ScanApplicationsRequest}
     */
    public ScanApplicationsRequest build() {
      validate();
      return new ScanApplicationsRequest(namespaceId, application, scanFrom, scanTo,
          filters, sortOrder, limit, latestOnly, sortCreationTime);
    }

    private void validate() {
      // Validate namespace
      if (namespaceId != null) {
        if (scanFrom != null && !namespaceId.equals(scanFrom.getNamespaceId())) {
          throw new IllegalArgumentException(
              "Requested to start scan from application " + scanFrom
                  + " that is outside of scan namespace " + namespaceId
          );
        }

        if (scanTo != null && !namespaceId.equals(scanTo.getNamespaceId())) {
          throw new IllegalArgumentException("Requested to finish scan at application " + scanTo
              + " that is outside of scan namespace " + namespaceId
          );
        }
      }

      // Validate application reference
      if (application != null) {
        if (namespaceId == null) {
          throw new IllegalArgumentException(
              "Requested to scan application " + application + " without namespaceId");
        }

        if (scanFrom != null && !application.equals(scanFrom.getApplication())) {
          throw new IllegalArgumentException(
              "Requested to start scan from application ID " + scanFrom
                  + " that does not match application name" + application
          );
        }

        if (scanTo != null && !application.equals(scanTo.getApplication())) {
          throw new IllegalArgumentException(
              "Requested to finish scan at application ID " + scanTo
                  + " that does not match application name" + application
          );
        }
      }
    }
  }
}
