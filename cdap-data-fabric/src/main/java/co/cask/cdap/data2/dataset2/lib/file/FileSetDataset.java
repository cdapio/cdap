/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

  private final Location baseLocation;
  private final List<Location> inputLocations;
  private final Location outputLocation;

  private final ClassLoader classLoader;
  private final String inputFormatClassName;
  private final String outputFormatClassName;

  /**
   * Constructor.
   *
   * @param datasetContext the context for the dataset
   * @param cConf the CDAP configuration
   * @param name name of the dataset
   * @param locationFactory the location factory
   * @param properties the dataset's properties from the spec
   * @param runtimeArguments the runtime arguments
   * @param classLoader the class loader to instantiate the input and output format class
   */
  public FileSetDataset(DatasetContext datasetContext, CConfiguration cConf, String name,
                        LocationFactory locationFactory,
                        @Nonnull Map<String, String> properties,
                        @Nonnull Map<String, String> runtimeArguments,
                        @Nullable ClassLoader classLoader) throws IOException {

    Preconditions.checkNotNull(datasetContext, "Dataset context must not be null");
    Preconditions.checkNotNull(name, "Dataset name must not be null");
    Preconditions.checkArgument(!name.isEmpty(), "Dataset name must not be empty");
    Preconditions.checkNotNull(runtimeArguments, "Runtime arguments must not be null");
    Preconditions.checkNotNull(properties, "Dataset properties must not be null");
    Preconditions.checkNotNull(FileSetProperties.getBasePath(properties), "Base path must not be null");

    String namespaceId = datasetContext.getNamespaceId();
    this.properties = properties;
    String dataDir = cConf.get(Constants.Dataset.DATA_DIR, Constants.Dataset.DEFAULT_DATA_DIR);
    String basePath = FileSetProperties.getBasePath(properties);
    this.baseLocation = locationFactory.create(namespaceId).append(dataDir).append(basePath);
    this.runtimeArguments = runtimeArguments;
    this.classLoader = classLoader;
    this.inputFormatClassName = FileSetProperties.getInputFormat(properties);
    this.outputFormatClassName = FileSetProperties.getOutputFormat(properties);
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

  private <T> Class<? extends T> getFormat(String className, Class<?> baseClass, ClassLoader classLoader) {
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
      throw new DataSetException(baseClass.getName() + " class '" + className + "' not found. ", e);
    }
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
    return (Class<? extends T>) getFormat(inputFormatClassName, InputFormat.class, classLoader);
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return getInputFormatConfiguration(inputLocations);
  }

  @Override
  public Map<String, String> getInputFormatConfiguration(Iterable<? extends Location> inputLocs) {
    String inputs = Joiner.on(',').join(Iterables.transform(inputLocs, new Function<Location, String>() {
      @Override
      public String apply(@Nullable Location location) {
        return getFileSystemPath(location);
      }
    }));
    Map<String, String> config = Maps.newHashMap();
    config.putAll(FileSetProperties.getInputProperties(properties));
    config.putAll(FileSetProperties.getInputProperties(runtimeArguments));
    config.put("mapred.input.dir", inputs);
    return ImmutableMap.copyOf(config);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Class<? extends T> getOutputFormatClass() {
    return (Class<? extends T>) getFormat(outputFormatClassName, OutputFormat.class, classLoader);
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    Map<String, String> config = Maps.newHashMap();
    config.putAll(FileSetProperties.getOutputProperties(properties));
    config.putAll(FileSetProperties.getOutputProperties(runtimeArguments));
    config.put(FileOutputFormat.OUTDIR, getFileSystemPath(outputLocation));
    return ImmutableMap.copyOf(config);
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  private String getFileSystemPath(Location loc) {
    return loc.toURI().getPath();
  }
}
