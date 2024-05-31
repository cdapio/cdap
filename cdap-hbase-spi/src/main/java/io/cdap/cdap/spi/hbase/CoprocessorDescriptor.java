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

package io.cdap.cdap.spi.hbase;

import io.cdap.cdap.api.annotation.Beta;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Describes HBase coprocessor.
 */
@Beta
public final class CoprocessorDescriptor {

  private final String className;
  private final String path;
  private final int priority;
  private final Map<String, String> properties;

  public CoprocessorDescriptor(String className, @Nullable String path, int priority,
      Map<String, String> properties) {
    this.className = className;
    this.path = path;
    this.priority = priority;
    this.properties = properties == null ? Collections.<String, String>emptyMap()
        : Collections.unmodifiableMap(properties);
  }

  public String getClassName() {
    return className;
  }

  @Nullable
  public String getPath() {
    return path;
  }

  public int getPriority() {
    return priority;
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
