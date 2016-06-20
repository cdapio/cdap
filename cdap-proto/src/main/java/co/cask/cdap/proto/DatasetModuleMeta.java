/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.proto;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Dataset module meta data
 */
public class DatasetModuleMeta {
  private final String name;
  private final String className;
  // TODO: change type to Location
  private final URI jarLocation;

  private final List<String> types;
  private final List<String> usesModules;
  private final List<String> usedByModules;

  /**
   * Creates instance of {@link DatasetModuleMeta}
   * @param name name of the dataset module
   * @param className class name of the dataset module
   * @param jarLocation location of the dataset module jar. {@code null} means this is "system module" which classes
   *                    always present in classpath. This helps to minimize redundant copying of jars.
   * @param types list of types announced by this module in the order they are announced
   * @param usesModules list of modules that this module depends on, ordered in a way they must be
   *                    loaded and initialized
   */
  public DatasetModuleMeta(String name, String className, @Nullable URI jarLocation,
                           Collection<String> types, Collection<String> usesModules) {
    this(name, className, jarLocation, types, usesModules, new ArrayList<String>());
  }

  /**
   * Creates instance of {@link DatasetModuleMeta}
   * @param name name of the dataset module
   * @param className class name of the dataset module
   * @param jarLocation location of the dataset module jar. {@code null} means this is "system module" which classes
   *                    always present in classpath. This helps to minimize redundant copying of jars.
   * @param types list of types announced by this module in the order they are announced
   * @param usesModules list of modules that this module depends on, ordered in a way they must be
   *                    loaded and initialized
   * @param usedByModules list of modules that depend on this module
   */
  public DatasetModuleMeta(String name, String className, @Nullable URI jarLocation,
                           Collection<String> types, Collection<String> usesModules,
                           Collection<String> usedByModules) {
    this.name = name;
    this.className = className;
    this.jarLocation = jarLocation;
    this.types = Collections.unmodifiableList(new ArrayList<>(types));
    this.usesModules = Collections.unmodifiableList(new ArrayList<>(usesModules));
    this.usedByModules = new ArrayList<>(usedByModules);
  }

  /**
   * @return name of the dataset module
   */
  public String getName() {
    return name;
  }

  /**
   * @return class name of the dataset module
   */
  public String getClassName() {
    return className;
  }

  /**
   * @return location of the dataset module jar, {@code null} means this is "system module" which classes always present
   *         in classpath. This helps to minimize redundant copying of jars
   */
  @Nullable
  public URI getJarLocation() {
    return jarLocation;
  }

  /**
   * @return list of types announced by this module in the order they are announced
   */
  public List<String> getTypes() {
    return types;
  }

  /**
   * @return list of modules this module depends on, ordered in a way they must be loaded and initialized
   */
  public List<String> getUsesModules() {
    return Collections.unmodifiableList(usesModules);
  }

  /**
   * @return list of modules that depend on this module
   */
  public Collection<String> getUsedByModules() {
    return Collections.unmodifiableCollection(usedByModules);
  }

  /**
   * Adds module to the list of dependant modules
   * @param name name of the dependant module to add
   */
  public void addUsedByModule(String name) {
    this.usedByModules.add(name);
  }

  /**
   * Removes module from the list of dependant modules
   * @param name name of the dependant module to remove
   */
  public void removeUsedByModule(String name) {
    this.usedByModules.remove(name);
  }

  @Override
  public String toString() {
    return "DatasetModuleMeta{" +
      "name='" + name + '\'' +
      ", className='" + className + '\'' +
      ", jarLocation=" + jarLocation +
      ", types=" + types +
      ", usesModules=" + usesModules +
      ", usedByModules=" + usedByModules +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatasetModuleMeta that = (DatasetModuleMeta) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(className, that.className) &&
      Objects.equals(types, that.types) &&
      Objects.equals(usesModules, that.usesModules) &&
      Objects.equals(usedByModules, that.usedByModules);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, className, types, usesModules, usedByModules);
  }
}
