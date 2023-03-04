/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.api.connector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The browse result for the given request. If the given path is browsable, this will contain
 * entities in that path. If it is not browsable, this will contain information on the path itself.
 */
public class BrowseDetail {

  // this count represents the total count of entities, when pagination is added in the future,
  // this count might not be equal to entities.size()
  private final int totalCount;
  // explore entity type -> sample property
  private final Set<BrowseEntityTypeInfo> sampleProperties;
  private final List<BrowseEntity> entities;
  // this will list all the available properties from the entities so UI knows what to expect
  private final Set<String> propertyHeaders;

  private BrowseDetail(int totalCount, Set<BrowseEntityTypeInfo> sampleProperties,
      List<BrowseEntity> entities, Set<String> propertyHeaders) {
    this.totalCount = totalCount;
    this.sampleProperties = sampleProperties;
    this.entities = entities;
    this.propertyHeaders = propertyHeaders;
  }

  public Collection<BrowseEntityTypeInfo> getSampleProperties() {
    return sampleProperties;
  }

  public int getTotalCount() {
    return totalCount;
  }

  public List<BrowseEntity> getEntities() {
    return entities;
  }

  public Set<String> getPropertyHeaders() {
    return propertyHeaders;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BrowseDetail that = (BrowseDetail) o;
    return totalCount == that.totalCount
        && Objects.equals(sampleProperties, that.sampleProperties)
        && Objects.equals(entities, that.entities)
        && Objects.equals(propertyHeaders, that.propertyHeaders);
  }

  @Override
  public int hashCode() {
    return Objects.hash(totalCount, sampleProperties, entities, propertyHeaders);
  }

  /**
   * Get the builder to build this object
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link BrowseDetail}
   */
  public static class Builder {

    private int totalCount;
    private Set<BrowseEntityTypeInfo> sampleProperties;
    private List<BrowseEntity> entities;

    public Builder() {
      this.sampleProperties = new HashSet<>();
      this.entities = new ArrayList<>();
    }

    public Builder setTotalCount(int totalCount) {
      this.totalCount = totalCount;
      return this;
    }

    public Builder setSampleProperties(Collection<BrowseEntityTypeInfo> sampleProperties) {
      this.sampleProperties.clear();
      this.sampleProperties.addAll(sampleProperties);
      return this;
    }

    public Builder setEntities(List<BrowseEntity> entities) {
      this.entities.clear();
      this.entities.addAll(entities);
      return this;
    }

    public Builder addEntity(BrowseEntity entity) {
      this.entities.add(entity);
      return this;
    }

    public Builder addEntities(List<BrowseEntity> entities) {
      this.entities.addAll(entities);
      return this;
    }

    public BrowseDetail build() {
      Set<String> propertyHeaders = new HashSet<>();
      entities.forEach(entity -> {
        propertyHeaders.addAll(entity.getProperties().keySet());
      });
      return new BrowseDetail(totalCount, sampleProperties, entities, propertyHeaders);
    }
  }
}
