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

package co.cask.cdap.api;

import co.cask.cdap.api.plugin.Plugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * d
 */
public class AbstractProgramSpecification implements ProgramSpecification {
  private final String className;
  private final String name;
  private final String description;
  private final Map<String, Plugin> plugins;

  public AbstractProgramSpecification(String className, String name, String description, Map<String, Plugin> plugins) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.plugins = plugins.isEmpty() ? Collections.emptyMap() :
      Collections.unmodifiableMap(new HashMap<>(plugins));
  }

  public AbstractProgramSpecification(String className, String name, String description) {
    this(className, name, description, Collections.emptyMap());
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Map<String, Plugin> getPlugins() {
    return plugins;
  }

  @Override
  public String toString() {
    return "AbstractProgramSpecification{" +
      "className='" + className + '\'' +
      ", name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", plugins=" + plugins +
      '}';
  }
}
