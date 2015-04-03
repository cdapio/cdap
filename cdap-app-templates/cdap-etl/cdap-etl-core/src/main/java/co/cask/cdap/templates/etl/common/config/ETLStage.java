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

package co.cask.cdap.templates.etl.common.config;

import com.google.common.base.Objects;

import java.util.Map;

/**
 * ETL Stage Configuration.
 */
public class ETLStage {
  private final String name;
  private final Map<String, String> properties;

  public ETLStage(String name, Map<String, String> properties) {
    this.name = name;
    this.properties = properties;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || o.getClass() != this.getClass()) {
      return false;
    }

    ETLStage other = (ETLStage) o;
    return Objects.equal(this.name, other.name) && Objects.equal(this.properties, other.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, properties);
  }
}
