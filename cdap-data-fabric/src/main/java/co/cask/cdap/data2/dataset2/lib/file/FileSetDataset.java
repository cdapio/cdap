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

package co.cask.cdap.data2.dataset2.lib.file;

import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Implementation of file dataset.
 */
public final class FileSetDataset implements FileSet {

  private final Map<String, String> properties;
  private final Map<String, String> runtimeArguments;

  private final String name;
  private final Location baseLocation;
  private final List<Location> inputLocations;
  private final Location outputLocation;

  private final Class<? extends InputFormat> inputFormatClass;
  private final Class<? extends OutputFormat> outputFormatClass;

  /**
   * Contructor.
   * @param name name of the dataset
   * @param locationFactory the location factory
   * @param properties the dataset's properties from the spec
   * @param runtimeArguments the runtime arguments
   * @param classLoader the class loader to instantiate the input and output format class
   */
  public FileSetDataset(String name, LocationFactory locationFactory,
                        @Nonnull Map<String, String> properties,
                        @Nonnull Map<String, String> runtimeArguments,
                        @Nullable ClassLoader classLoader) {

    Preconditions.checkNotNull(name, "Dataset name must not be null");
    Preconditions.checkArgument(!name.isEmpty(), "Dataset name must not be empty");
    Preconditions.checkNotNull(runtimeArguments, "Runtime arguments must not be null");
    Preconditions.checkNotNull(properties, "Dataset properties must not be null");
    Preconditions.checkNotNull(properties.get(FileSetProperties.BASE_PATH), "Base path must not be null");

    this.name = name;
    this.properties = properties;
    this.baseLocation = locationFactory.create(properties.get(FileSetProperties.BASE_PATH));
    this.runtimeArguments = runtimeArguments;
    this.inputFormatClass = getFormat(FileSetProperties.INPUT_FORMAT, InputFormat.class, classLoader);
    this.outputFormatClass = getFormat(FileSetProperties.OUTPUT_FORMAT, OutputFormat.class, classLoader);
    this.outputLocation = determineOutputLocation();
    this.inputLocations = determineInputLocations();
  }

  private Location determineOutputLocation() {
    String outputPath = FileSetArguments.getOutputPath(runtimeArguments);
    return outputPath == null ? baseLocation : createLocation(outputPath);
  }

  private List<Location> determineInputLocations() {
    Collection<String> inputPaths = FileSetArguments.getInputPaths(runtimeArguments);
    if (inputPaths == null) {
      return Collections.singletonList(baseLocation);
    } else {
      List<Location> locations = Lists.newLinkedList();
      for (String path : inputPaths) {
        locations.add(createLocation(path));
      }
      return locations;
    }
  }

  private Location createLocation(String relativePath) {
    try {
      return baseLocation.append(relativePath);
    } catch (IOException e) {
      throw new DataSetException("Error constructing path from base '" + baseLocation.toURI().getPath() +
                                   "' and relative path '" + relativePath + "'", e);
    }
  }

  private <T> Class<? extends T> getFormat(String propertyName, Class<?> baseClass, ClassLoader classLoader) {
    String className = properties.get(propertyName);
    if (className == null) {
      return null;
    }
    try {
      Class<?> actualClass = classLoader == null
        ? Class.forName(className)
        : classLoader.loadClass(className);

      if (baseClass.isAssignableFrom(actualClass)) {
        @SuppressWarnings("unchecked")
        Class<? extends T> returnClass = (Class<? extends T>) actualClass;
        return returnClass;
      } else {
        throw new DataSetException("Class '" + className + "' does not extend " + baseClass.getName());
      }
    } catch (ClassNotFoundException e) {
      throw new DataSetException(propertyName + " class '" + className + "' not found. ", e);
    }
  }

  private Map<String, String> getFormatConfiguration(String propertyPrefix, Map<String, String> additional) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().startsWith(propertyPrefix)) {
        builder.put(entry.getKey().substring(propertyPrefix.length()), entry.getValue());
      }
    }
    for (Map.Entry<String, String> entry : runtimeArguments.entrySet()) {
      if (entry.getKey().startsWith(propertyPrefix)) {
        builder.put(entry.getKey().substring(propertyPrefix.length()), entry.getValue());
      }
    }
    builder.putAll(additional);
    return builder.build();
  }

  @Override
  public Location getBaseLocation() {
    return baseLocation;
  }

  @Override
  public List<Location> getInputLocations() {
    return Lists.newLinkedList(inputLocations);
  }

  @Override
  public Location getOutputLocation() {
    return outputLocation;
  }

  @Override
  public Location getLocation(String relativePath) {
    return createLocation(relativePath);
  }

  @Override
  public void close() throws IOException {
    // no-op - nothing to do
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Class<? extends T> getInputFormatClass() {
    return (Class<? extends T>) inputFormatClass;
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    StringBuilder paths = new StringBuilder();
    String sep = "";
    for (Location location : inputLocations) {
      paths.append(sep).append(getFileSystemPath(location));
      sep = ",";
    }
    return getFormatConfiguration(FileSetProperties.INPUT_PROPERTIES_PREFIX,
                                  ImmutableMap.of("mapred.input.dir", paths.toString()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Class<? extends T> getOutputFormatClass() {
    return (Class<? extends T>) outputFormatClass;
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return getFormatConfiguration(FileSetProperties.OUTPUT_PROPERTIES_PREFIX,
                                  ImmutableMap.of(FileOutputFormat.OUTDIR, getFileSystemPath(outputLocation)));
  }

  private String getFileSystemPath(Location loc) {
    return loc.toURI().getPath();
  }

  public String getName() {
    return name;
  }
}
