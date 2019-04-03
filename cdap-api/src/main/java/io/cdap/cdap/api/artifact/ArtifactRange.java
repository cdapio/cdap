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

package co.cask.cdap.api.artifact;

import java.util.Objects;

/**
 * Represents a range of versions for an artifact. The lower version is inclusive and the upper version is exclusive.
 */
public class ArtifactRange extends ArtifactVersionRange {
  private final String namespace;
  private final String name;

  public ArtifactRange(String namespace, String name, ArtifactVersion lower, ArtifactVersion upper) {
    this(namespace, name, lower, true, upper, false);
  }

  public ArtifactRange(String namespace, String name, ArtifactVersion lower, boolean isLowerInclusive,
                       ArtifactVersion upper, boolean isUpperInclusive) {
    super(lower, isLowerInclusive, upper, isUpperInclusive);
    this.namespace = namespace;
    this.name = name;
  }

  public ArtifactRange(String namespace, String name, ArtifactVersionRange range) {
    this(namespace, name, range.lower, range.isLowerInclusive, range.upper, range.isUpperInclusive);
  }

  /**
   * get the namespace the artifact belongs to
   * @return namespace
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * get the name of the artifact
   * @return name of artifact
   */
  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactRange that = (ArtifactRange) o;

    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(name, that.name) &&
      Objects.equals(lower, that.lower) &&
      Objects.equals(upper, that.upper) &&
      isLowerInclusive == that.isLowerInclusive &&
      isUpperInclusive == that.isUpperInclusive;
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name, lower, isLowerInclusive, upper, isUpperInclusive);
  }

  @Override
  public String toString() {
    return toString(new StringBuilder().append(namespace).append(':'));
  }

  private String toString(StringBuilder builder) {
    return builder
      .append(name)
      .append(isLowerInclusive ? '[' : '(')
      .append(lower.getVersion())
      .append(',')
      .append(upper.getVersion())
      .append(isUpperInclusive ? ']' : ')')
      .toString();
  }
}
