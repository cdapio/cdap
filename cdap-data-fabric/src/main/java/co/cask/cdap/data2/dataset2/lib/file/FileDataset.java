/*
 * Copyright 2014 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.File;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Implementation of file dataset.
 */
public final class FileDataset implements File {

  private final Map<String, String> properties;
  private final Map<String, String> runtimeArguments;

  private final String name;
  private final Location baseLocation;
  private final List<Location> inputLocations;
  private final Location outputLocation;

  private final Class<? extends InputFormat> inputFormatClass;
  private final Class<? extends OutputFormat> outputFormatClass;

  public FileDataset(String name, LocationFactory locationFactory,
                     Map<String, String> properties, Map<String, String> runtimeArguments) {

    Preconditions.checkNotNull(name, "Dataset name must not be null");
    Preconditions.checkArgument(!name.isEmpty(), "Dataset name must not be empty");
    Preconditions.checkNotNull(properties, "Dataset properties must not be null");
    Preconditions.checkNotNull(properties.get(PROPERTY_BASE_PATH), "Base path must not be null");

    this.name = name;
    this.properties = properties;
    this.baseLocation = locationFactory.create(properties.get(PROPERTY_BASE_PATH));
    this.runtimeArguments = runtimeArguments == null ? Collections.<String, String>emptyMap() : runtimeArguments;
    this.inputFormatClass = getFormat(PROPERTY_INPUT_FORMAT, InputFormat.class);
    this.outputFormatClass = getFormat(PROPERTY_OUTPUT_FORMAT, OutputFormat.class);
    this.outputLocation = determineOutputLocation();
    this.inputLocations = determineInputLocations();
  }

  private Location determineOutputLocation() {
    String outputPath = runtimeArguments.get(ARGUMENT_OUTPUT_PATH);
    return outputPath == null ? baseLocation : createLocation(outputPath);
  }

  private List<Location> determineInputLocations() {
    String inputPaths = runtimeArguments.get(ARGUMENT_INPUT_PATHS);
    if (inputPaths == null) {
      return Collections.singletonList(baseLocation);
    } else {
      List<Location> locations = Lists.newLinkedList();
      for (String path : inputPaths.split(",")) {
        locations.add(createLocation(path.trim()));
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

  private <T> Class<? extends T> getFormat(String propertyName, Class<?> baseClass) {
    String className = properties.get(propertyName);
    if (className == null) {
      return null;
    }
    try {
      Class<?> actualClass = Class.forName(className);
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
    boolean first = true;
    for (Location location : inputLocations) {
      if (first) {
        first = false;
      } else {
        paths.append(",");
      }
      paths.append(getFileSystemPath(location));
    }
    return getFormatConfiguration(PROPERTY_INPUT_PROPERTIES_PREFIX,
                                  ImmutableMap.of("mapred.input.dir", paths.toString()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Class<? extends T> getOutputFormatClass() {
    return (Class<? extends T>) outputFormatClass;
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return getFormatConfiguration(PROPERTY_OUTPUT_PROPERTIES_PREFIX,
                                  ImmutableMap.of(FileOutputFormat.OUTDIR, getFileSystemPath(outputLocation)));
  }

  private String getFileSystemPath(Location loc) {
    return loc.toURI().getPath();
  }

  public String getName() {
    return name;
  }
}
