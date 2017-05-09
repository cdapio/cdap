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

package co.cask.cdap.api.artifact;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.schema.Schema;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Contains information about an Application class.
 */
@Beta
public final class ApplicationClass {
  private final String className;
  private final String description;
  private final Schema configSchema;

  public ApplicationClass(String className, String description, @Nullable Schema configSchema) {
    if (description == null) {
      throw new IllegalArgumentException("Application class description cannot be null");
    }
    if (className == null) {
      throw new IllegalArgumentException("Application class className cannot be null");
    }
    this.className = className;
    this.description = description;
    this.configSchema = configSchema;
  }

  /**
   * Returns description of the Application Class.
   */
  public String getDescription() {
    return description;
  }

  /**
   * Returns the fully qualified class name of the Application Class.
   */
  public String getClassName() {
    return className;
  }

  /**
   * Returns the schema of the Application config, or null if the Application does not use config
   */
  @Nullable
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

    ApplicationClass that = (ApplicationClass) o;

    return Objects.equals(description, that.description) &&
      Objects.equals(className, that.className) &&
      Objects.equals(configSchema, that.configSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, description, configSchema);
  }

  @Override
  public String toString() {
    return "ApplicationClass{" +
      "className='" + className + '\'' +
      ", description='" + description + '\'' +
      ", configSchema=" + configSchema +
      '}';
  }
}
