/*
 * Copyright © 2025 Cask Data, Inc.
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

import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Defines parameters for application count in a store.
 */
public class CountApplicationsRequest {
  @Nullable
  private final NamespaceId namespaceId;
  @Nullable
  private final String application;
  private final List<ApplicationFilter> filters;
  private final boolean latestOnly;

  /** Constructor for the count apps request.
   *
   * @param namespaceId namespace to return applications for or null for all namespaces
   * @param application application to return applications for
   * @param filters additional filters to apply
   */
  private CountApplicationsRequest(@Nullable NamespaceId namespaceId,
      @Nullable String application,
      List<ApplicationFilter> filters,
      boolean latestOnly) {
    this.namespaceId = namespaceId;
    this.application = application;
    this.filters = filters;
    this.latestOnly = latestOnly;
  }

  /**
   * Get namespace for which the applications are to be counted.
   *
   * @return namespace to return applications count for, or null for all namespaces
   */
  @Nullable
  public NamespaceId getNamespaceId() {
    return namespaceId;
  }

  /**
   * Get the application name whose versions are to be counted.
   *
   * @return application name to count versions for, or null if all applications are considered.
   */
  @Nullable
  public String getApplication() {
    return application;
  }


  /**
   * Get the list of filters to be applied.
   *
   * @return additional filters to apply. All filters must be satisfied (AND operation). For
   *     performance reasons it's better to put {@link ApplicationFilter.ArtifactIdFilter} first.
   */
  public List<ApplicationFilter> getFilters() {
    return filters;
  }

  /**
   * Get the latestOnly flag.
   *
   * @return whether to return the latest version of an application
   */
  public boolean getLatestOnly() {
    return latestOnly;
  }

  @Override
  public String toString() {
    return "ScanApplicationsRequest{"
        + "namespaceId=" + namespaceId
        + ", application=" + application
        + ", filters=" + filters
        + ", latestOnly=" + latestOnly
        + '}';
  }

  /**
   * Get the builder object for the count apps request.
   *
   * @return builder to create a new {@link CountApplicationsRequest}
   */
  public static CountApplicationsRequest.Builder builder() {
    return new CountApplicationsRequest.Builder();
  }

  /**
   * Get a prefilled builder object for the count apps request.
   *
   * @param request original request to use as a template
   * @return builder to create a new {@link CountApplicationsRequest} prefilled with passed in
   *     request values
   */
  public static CountApplicationsRequest.Builder builder(CountApplicationsRequest request) {
    return new CountApplicationsRequest.Builder(request);
  }

  /**
   * Defined the builder class for the {@link CountApplicationsRequest}.
   */
  public static class Builder {
    @Nullable
    private NamespaceId namespaceId;
    @Nullable
    private String application;
    private List<ApplicationFilter> filters = new ArrayList<>();
    private boolean latestOnly;

    private Builder() {
    }

    private Builder(CountApplicationsRequest request) {
      this.namespaceId = request.namespaceId;
      this.application = request.application;
      this.filters = request.filters;
      this.latestOnly = request.latestOnly;
    }

    /**
     * Set the namespace to count applications in.
     *
     * @param namespaceId namespace to count applications in
     */
    public CountApplicationsRequest.Builder setNamespaceId(NamespaceId namespaceId) {
      this.namespaceId = namespaceId;
      return this;
    }

    /**
     * Set the application reference to count the versions of.
     *
     * @param applicationReference application to count without version
     */
    public CountApplicationsRequest.Builder setApplicationReference(
        ApplicationReference applicationReference) {
      this.namespaceId = applicationReference.getNamespaceId();
      this.application = applicationReference.getApplication();
      return this;
    }

    /**
     * Add a filter for the applications to be considered for counting.
     *
     * @param filter adds a filter
     */
    public CountApplicationsRequest.Builder addFilter(ApplicationFilter filter) {
      this.filters.add(filter);
      return this;
    }

    /**
     * Add a collection of filters for the applications to be considered for counting.
     *
     * @param filters adds multiple filters
     */
    public CountApplicationsRequest.Builder addFilters(Collection<ApplicationFilter> filters) {
      this.filters.addAll(filters);
      return this;
    }

    /**
     * Set the latestOnly flag. If set, only the latest version of each application is considered.
     *
     * @param latestOnly whether to count only the latest version of an application
     */
    public CountApplicationsRequest.Builder setLatestOnly(boolean latestOnly) {
      this.latestOnly = latestOnly;
      return this;
    }

    /**
     * Build the CountApplicationRequest.
     *
     * @return new {@link CountApplicationsRequest}
     */
    public CountApplicationsRequest build() {
      validate();
      return new CountApplicationsRequest(namespaceId, application, filters, latestOnly);
    }

    private void validate() {
      // Validate application reference
      if (application != null) {
        if (namespaceId == null) {
          throw new IllegalArgumentException(
              "Requested to count application " + application + " without namespaceId");
        }
      }
    }
  }
}
