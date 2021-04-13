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

import java.util.Collection;
import java.util.Objects;

/**
 * An entity that is explorable or samplable, or both.
 * For example, for file connector, this can be file or directory.
 * For database connector, this can be database or table.
 */
public class ExploreEntity {
  private final String name;
  private final String path;
  private final String type;
  private final boolean ableToSample;
  private final boolean ableToExplore;
  private final Collection<ExploreEntityProperty> properties;

  public ExploreEntity(String name, String path, String type, boolean ableToSample, boolean ableToExplore,
                       Collection<ExploreEntityProperty> properties) {
    this.name = name;
    this.path = path;
    this.type = type;
    this.ableToSample = ableToSample;
    this.ableToExplore = ableToExplore;
    this.properties = properties;
  }

  public String getName() {
    return name;
  }

  public String getPath() {
    return path;
  }

  public String getType() {
    return type;
  }

  public boolean isAbleToSample() {
    return ableToSample;
  }

  public boolean isAbleToExplore() {
    return ableToExplore;
  }

  public Collection<ExploreEntityProperty> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ExploreEntity that = (ExploreEntity) o;
    return ableToSample == that.ableToSample &&
      ableToExplore == that.ableToExplore &&
      Objects.equals(name, that.name) &&
      Objects.equals(path, that.path) &&
      Objects.equals(type, that.type) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, path, type, ableToSample, ableToExplore, properties);
  }
}
