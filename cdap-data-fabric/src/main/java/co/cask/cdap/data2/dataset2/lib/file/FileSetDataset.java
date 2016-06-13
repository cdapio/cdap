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

package co.cask.cdap.data2.dataset2.lib.file;

import co.cask.cdap.api.data.batch.DatasetOutputCommitter;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.proto.Id;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Implementation of file dataset.
 */
public final class FileSetDataset implements FileSet, DatasetOutputCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(FileSetDataset.class);

  public static final String FILESET_VERSION_PROPERTY = "fileset.version";
  static final String FILESET_VERSION = "2";

  private final DatasetSpecification spec;
  private final Map<String, String> runtimeArguments;
  private final boolean isExternal;
  private final Location baseLocation;
  private final List<Location> inputLocations;
  private final Location outputLocation;
  private final String inputFormatClassName;
  private final String outputFormatClassName;

  /**
   * Constructor.
   * @param datasetContext the context for the dataset
   * @param cConf the CDAP configuration
   * @param spec the dataset specification
   * @param namespacedLocationFactory a factory for namespaced {@link Location}
   * @param runtimeArguments the runtime arguments
   */
  public FileSetDataset(DatasetContext datasetContext, CConfiguration cConf,
                        DatasetSpecification spec,
                        LocationFactory absoluteLocationFactory,
                        NamespacedLocationFactory namespacedLocationFactory,
                        @Nonnull Map<String, String> runtimeArguments) throws IOException {

    Preconditions.checkNotNull(datasetContext, "Dataset context must not be null");
    Preconditions.checkNotNull(runtimeArguments, "Runtime arguments must not be null");

    this.spec = spec;
    this.runtimeArguments = runtimeArguments;
    this.isExternal = FileSetProperties.isDataExternal(spec.getProperties());
    this.baseLocation = determineBaseLocation(datasetContext, cConf, spec,
                                              absoluteLocationFactory, namespacedLocationFactory);
    this.outputLocation = determineOutputLocation();
    this.inputLocations = determineInputLocations();
    this.inputFormatClassName = FileSetProperties.getInputFormat(spec.getProperties());
    this.outputFormatClassName = FileSetProperties.getOutputFormat(spec.getProperties());
  }

  /**
   * Generate the base location of the file set.
   * <ul>
   *   <li>If the properties do not contain a base path, generate one from the dataset name;</li>
   *   <li>If the base path is absolute, return a location relative to the root of the file system;</li>
   *   <li>Otherwise return a location relative to the data directory of the namespace.</li>
   * </ul>
   * This is package visible, because FileSetAdmin needs it, too.
   * TODO: Ideally, this should be done in configure(), but currently it cannot because of CDAP-1721
   */
  static Location determineBaseLocation(DatasetContext datasetContext, CConfiguration cConf,
                                        DatasetSpecification spec, LocationFactory rootLocationFactory,
                                        NamespacedLocationFactory namespacedLocationFactory) throws IOException {

    // older versions of file set incorrectly interpret absolute paths as relative to the namespace's
    // data directory. These file sets do not have the file set version property.
    boolean hasAbsoluteBasePathBug = spec.getProperties().get(FILESET_VERSION_PROPERTY) == null;

    String basePath = FileSetProperties.getBasePath(spec.getProperties());
    if (basePath == null) {
      basePath = spec.getName().replace('.', '/');
    }
    // for absolute paths, get the location from the file system's root.
    if (basePath.startsWith("/")) {
      // but only if it is not a legacy dataset that interprets absolute paths as relative
      if (hasAbsoluteBasePathBug) {
        LOG.info("Dataset {} was created with a version of FileSet that treats absolute path {} as relative. " +
                   "To disable this message, upgrade the dataset properties with a relative path. ",
                 spec.getName(), basePath);
      } else {
        String topLevelPath = namespacedLocationFactory.getBaseLocation().toURI().getPath();
        topLevelPath = topLevelPath.endsWith("/") ? topLevelPath : topLevelPath + "/";
        Location baseLocation = rootLocationFactory.create(basePath);
        if (baseLocation.toURI().getPath().startsWith(topLevelPath)) {
          throw new DataSetException("Invalid base path '" + basePath + "' for dataset '" + spec.getName() + "'. " +
                                       "It must not be inside the CDAP base path '" + topLevelPath + "'.");
        }
        return baseLocation;
      }
    }
    Id.Namespace namespaceId = Id.Namespace.from(datasetContext.getNamespaceId());
    String dataDir = cConf.get(Constants.Dataset.DATA_DIR, Constants.Dataset.DEFAULT_DATA_DIR);
    return namespacedLocationFactory.get(namespaceId).append(dataDir).append(basePath);
  }

  private Location determineOutputLocation() {
    if (FileSetArguments.isBaseOutputPath(runtimeArguments)) {
      return baseLocation;
    }
    String outputPath = FileSetArguments.getOutputPath(runtimeArguments);
    return outputPath == null ? null : createLocation(outputPath);
  }

  private List<Location> determineInputLocations() {
    List<Location> locations = Lists.newLinkedList();
    for (String path : FileSetArguments.getInputPaths(runtimeArguments)) {
      locations.add(createLocation(path));
    }
    return locations;
  }

  private Location createLocation(String relativePath) {
    try {
      return baseLocation.append(relativePath);
    } catch (IOException e) {
      throw new DataSetException("Error constructing path from base '" + baseLocation +
                                   "' and relative path '" + relativePath + "'", e);
    }
  }

  @Override
  public Location getBaseLocation() {
    // TODO: if the file set is external, we could return a ReadOnlyLocation that prevents writing [CDAP-2934]
    return baseLocation;
  }

  @Override
  public List<Location> getInputLocations() {
    // TODO: if the file set is external, we could return a ReadOnlyLocation that prevents writing [CDAP-2934]
    return Lists.newLinkedList(inputLocations);
  }

  @Override
  public Location getOutputLocation() {
    if (isExternal) {
      throw new UnsupportedOperationException(
        "Output is not supported for external file set '" + spec.getName() + "'");
    }
    return outputLocation;
  }

  @Override
  public Location getLocation(String relativePath) {
    // TODO: if the file set is external, we could return a ReadOnlyLocation that prevents writing [CDAP-2934]
    return createLocation(relativePath);
  }

  @Override
  public void close() throws IOException {
    // no-op - nothing to do
  }

  @Override
  public String getInputFormatClassName() {
    return inputFormatClassName;
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return getInputFormatConfiguration(inputLocations);
  }

  @Override
  public Map<String, String> getInputFormatConfiguration(Iterable<? extends Location> inputLocs) {
    Map<String, String> config = new HashMap<>();
    config.putAll(FileSetProperties.getInputProperties(spec.getProperties()));
    // runtime arguments may override the input properties
    config.putAll(FileSetProperties.getInputProperties(runtimeArguments));
    String inputs = Joiner.on(',').join(Iterables.transform(inputLocs, new Function<Location, String>() {
      @Override
      public String apply(@Nullable Location location) {
        return getFileSystemPath(location);
      }
    }));
    config.put(FileInputFormat.INPUT_DIR, inputs);
    return config;
  }

  @Override
  public String getOutputFormatClassName() {
    if (isExternal) {
      throw new UnsupportedOperationException(
        "Output is not supported for external file set '" + spec.getName() + "'");
    }
    return outputFormatClassName;
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    if (isExternal) {
      throw new UnsupportedOperationException(
        "Output is not supported for external file set '" + spec.getName() + "'");
    }
    Map<String, String> config = new HashMap<>();
    config.putAll(FileSetProperties.getOutputProperties(spec.getProperties()));
    // runtime arguments may override the output properties
    config.putAll(FileSetProperties.getOutputProperties(runtimeArguments));
    if (outputLocation != null) {
      config.put(FileOutputFormat.OUTDIR, getFileSystemPath(outputLocation));
    }
    return config;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  private String getFileSystemPath(Location loc) {
    return loc.toURI().getPath();
  }

  @Override
  public void onSuccess() throws DataSetException {
    // nothing needed to do on success
  }

  @Override
  public void onFailure() throws DataSetException {
    Location outputLocation = getOutputLocation();
    // If there is no output path, it is either using DynamicPartitioner or the job would have failed.
    // Either way, we can't do much here.
    if (outputLocation == null) {
      return;
    }

    try {
      // Only delete the configured output directory, if it is empty.
      // On Failure, org.apache.hadoop.mapreduce.lib.output.FileOutputFormat will remove files that it wrote,
      // but it leaves around the directory that it created.
      // We don't want to unconditionally delete the output directory on failure, because it may have files written
      // by a different job.
      if (outputLocation.isDirectory() && outputLocation.list().isEmpty()) {
        if (!outputLocation.delete()) {
          throw new DataSetException(String.format("Error deleting file(s) at path %s.", outputLocation));
        }
      }
    } catch (IOException ioe) {
      throw new DataSetException(String.format("Error deleting file(s) at path %s.", outputLocation), ioe);
    }
  }
}
