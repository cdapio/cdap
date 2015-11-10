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

package co.cask.cdap.etl.common;

import com.google.common.base.Objects;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * ETL Stage Configuration.
 */
public final class ETLStage {
  private final String name;
  private final Map<String, String> properties;
  private final String errorDatasetName;

  public ETLStage(String name, Map<String, String> properties, @Nullable String errorDatasetName) {
    this.name = name;
    this.properties = properties;
    this.errorDatasetName = errorDatasetName;
  }

  public ETLStage(String name, Map<String, String> properties) {
    this(name, properties, null);
  }

  public String getName() {
    return name;
  }

  @Nullable
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("properties", properties)
      .toString();
  }

  @Nullable
  public String getErrorDatasetName() {
    return errorDatasetName;
  }
}
