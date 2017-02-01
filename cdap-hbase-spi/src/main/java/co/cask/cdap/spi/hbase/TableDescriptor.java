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

package co.cask.cdap.spi.hbase;

import co.cask.cdap.api.annotation.Beta;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Describes an HBase table.
 */
@Beta
public final class TableDescriptor {

  private final String namespace;
  private final String name;
  private final Map<String, ColumnFamilyDescriptor> families;
  private final Map<String, CoprocessorDescriptor> coprocessors;
  private final Map<String, String> properties;

  public TableDescriptor(String namespace, String name, Set<ColumnFamilyDescriptor> families,
                         Set<CoprocessorDescriptor> coprocessors, Map<String, String> properties) {
    this.namespace = namespace;
    this.name = name;

    this.families = new HashMap<>();
    for (ColumnFamilyDescriptor family : families) {
      this.families.put(family.getName(), family);
    }

    this.coprocessors = new HashMap<>();
    for (CoprocessorDescriptor coprocessor : coprocessors) {
      this.coprocessors.put(coprocessor.getClassName(), coprocessor);
    }

    this.properties = properties == null ? Collections.<String, String>emptyMap()
      : Collections.unmodifiableMap(properties);
  }

  public String getNamespace() {
    return namespace;
  }

  public String getName() {
    return name;
  }

  public Map<String, ColumnFamilyDescriptor> getFamilies() {
    return Collections.unmodifiableMap(families);
  }

  public Map<String, CoprocessorDescriptor> getCoprocessors() {
    return Collections.unmodifiableMap(coprocessors);
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
