package com.continuuity.data2.datafabric.dataset.type;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
                           List<String> types, List<String> usesModules) {
    this.name = name;
    this.className = className;
    this.jarLocation = jarLocation;
    this.types = Collections.unmodifiableList(types);
    this.usesModules = Collections.unmodifiableList(usesModules);
    this.usedByModules = Lists.newArrayList();
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
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("className", className)
      .add("jarLocation", jarLocation)
      .add("usesModules", Joiner.on(",").skipNulls().join(usesModules))
      .add("usedByModules", Joiner.on(",").skipNulls().join(usedByModules))
      .toString();
  }
}
