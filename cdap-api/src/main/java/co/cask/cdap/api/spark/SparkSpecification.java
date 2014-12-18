/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.spark;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.common.PropertyProvider;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * A default specification for {@link Spark} programs
 */
public final class SparkSpecification implements ProgramSpecification, PropertyProvider {

  private final String className;
  private final String name;
  private final String description;
  private final String mainClassName;
  private final Map<String, String> properties;

  public SparkSpecification(String className, String name, String description, String mainClassName,
                            @Nonnull Map<String, String> properties) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.mainClassName = mainClassName;
    this.properties = Collections.unmodifiableMap(properties);
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

  public String getMainClassName() {
    return mainClassName;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }
}
