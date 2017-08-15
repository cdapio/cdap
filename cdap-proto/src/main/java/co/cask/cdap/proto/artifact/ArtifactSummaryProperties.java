/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.proto.artifact;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An {@link ArtifactSummary} with properties.
 */
public class ArtifactSummaryProperties extends ArtifactSummary {
  private final Map<String, String> properties;

  public ArtifactSummaryProperties(String name, String version, ArtifactScope scope, Map<String, String> properties) {
    super(name, version, scope);
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
  }

  public Map<String, String> getProperties() {
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
    if (!super.equals(o)) {
      return false;
    }

    ArtifactSummaryProperties that = (ArtifactSummaryProperties) o;

    return Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), properties);
  }

  @Override
  public String toString() {
    return "ArtifactSummaryProperties{" +
      "properties=" + properties +
      "} " + super.toString();
  }
}
