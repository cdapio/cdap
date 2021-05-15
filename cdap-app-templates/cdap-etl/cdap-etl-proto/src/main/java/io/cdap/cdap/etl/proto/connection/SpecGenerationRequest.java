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

package io.cdap.cdap.etl.proto.connection;

import java.util.Map;
import java.util.Objects;

/**
 * Spec generation request expected from a http request
 */
public class SpecGenerationRequest {
  private final String path;
  private final Map<String, String> properties;

  public SpecGenerationRequest(String path, Map<String, String> properties) {
    this.path = path;
    this.properties = properties;
  }

  public String getPath() {
    return path;
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

    SpecGenerationRequest that = (SpecGenerationRequest) o;
    return Objects.equals(path, that.path) &&
             Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, properties);
  }
}
