/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.proto.summary;

import com.google.common.base.Objects;

/**
 * Represents an aggregate of program runs by the parent artifact.
 */
public class ArtifactAggregate extends ProgramRunAggregate {
  private final String name;
  private final String version;
  private final String scope;

  public ArtifactAggregate(String name, String version, String scope, long runs) {
    super(runs);
    this.name = name;
    this.version = version;
    this.scope = scope;
  }

  /**
   * @return the name of the artifact
   */
  public String getName() {
    return name;
  }

  /**
   * @return the version of the artifact
   */
  public String getVersion() {
    return version;
  }

  /**
   * @return the scope of the artifact
   */
  public String getScope() {
    return scope;
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

    ArtifactAggregate that = (ArtifactAggregate) o;
    return Objects.equal(name, that.name) &&
      Objects.equal(version, that.version) &&
      Objects.equal(scope, that.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), name, version, scope);
  }
}
