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

package co.cask.cdap.api.metadata;

import co.cask.cdap.api.annotation.Beta;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents metadata (properties and tags) of an entity.
 */
@Beta
public class Metadata {
  private final Map<String, String> properties;
  private final Set<String> tags;

  public Metadata(Map<String, String> properties, Set<String> tags) {
    this.properties = properties;
    this.tags = tags;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Set<String> getTags() {
    return tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Metadata)) {
      return false;
    }
    Metadata that = (Metadata) o;
    return Objects.equals(properties, that.properties) &&
      Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, tags);
  }

  @Override
  public String toString() {
    return "Metadata{" +
      "properties=" + properties +
      ", tags=" + tags +
      '}';
  }
}
