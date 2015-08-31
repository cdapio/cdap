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

package co.cask.cdap.dq;

import com.google.common.base.Objects;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Configuration class used for source of a DataQuality App
 */
public final class DataQualitySource {
  // Batch Source Plugin name - ex: 'Stream', 'Table' etc
  private final String name;
  // Unique ID used to identify this source
  private final String id;
  // Plugin Properties
  private final Map<String, String> properties;

  public DataQualitySource(String name, String id, Map<String, String> properties) {
    this.name = name;
    this.id = id;
    this.properties = properties;
  }

  public String getName() {
    return name;
  }

  public String getId() {
    return id;
  }

  @Nullable
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("id", id)
      .add("properties", properties)
      .toString();
  }
}
