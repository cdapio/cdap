/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.data.schema.Schema;

import java.util.Objects;

/**
 * Represents the objects returned by /classes/app/{classname}
 */
public class ApplicationClassInfo extends ApplicationClassSummary {
  private Schema configSchema;

  public ApplicationClassInfo(ArtifactSummary artifact, String className, Schema configSchema) {
    super(artifact, className);
    this.configSchema = configSchema;
  }

  public Schema getConfigSchema() {
    return configSchema;
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

    ApplicationClassInfo that = (ApplicationClassInfo) o;
    return super.equals(o) && Objects.equals(configSchema, that.configSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), configSchema);
  }
}
