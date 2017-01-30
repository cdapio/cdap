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

import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.WriteOnly;
import co.cask.cdap.api.data.batch.DatasetOutputCommitter;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.utils.FileUtils;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.twill.filesystem.ForwardingLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private final String permissions;

  /**
   * Constructor.
   * @param datasetContext the context for the dataset
   * @param cConf the CDAP configuration
   * @param spec the dataset specification
   * @param namespacedLocationFactory a factory for namespaced {@link Location}
   * @param runtimeArguments the runtime arguments
   */
  @SuppressWarnings("WeakerAccess")
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

    Location baseLocation = determineBaseLocation(datasetContext, cConf, spec,
                                                  absoluteLocationFactory, namespacedLocationFactory);
    this.baseLocation = new FileSetLocation(baseLocation,
                                            new FileSetLocationFactory(baseLocation.getLocationFactory()));
    this.outputLocation = determineOutputLocation();
    this.inputLocations = determineInputLocations();
    this.inputFormatClassName = FileSetProperties.getInputFormat(spec.getProperties());
    this.outputFormatClassName = FileSetProperties.getOutputFormat(spec.getProperties());

    // runtime arguments can override permissions
    this.permissions = runtimeArguments.containsKey(DatasetProperties.PROPERTY_PERMISSIONS)
      ? runtimeArguments.get(DatasetProperties.PROPERTY_PERMISSIONS)
      : spec.getProperty(DatasetProperties.PROPERTY_PERMISSIONS);
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
    NamespaceId namespaceId = new NamespaceId(datasetContext.getNamespaceId());
    String dataDir = cConf.get(Constants.Dataset.DATA_DIR, Constants.Dataset.DEFAULT_DATA_DIR);
    return namespacedLocationFactory.get(namespaceId.toId()).append(dataDir).append(basePath);
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
    return new FileSetLocation(baseLocation, new FileSetLocationFactory(baseLocation.getLocationFactory()));
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
    if (outputLocation != null) {
      config.put(FileOutputFormat.OUTDIR, getFileSystemPath(outputLocation));
    }
    // runtime arguments may override the permissions property
    Map<String, String> outputArguments = FileSetProperties.getOutputProperties(runtimeArguments);
    String outputPermissions = outputArguments.get(DatasetProperties.PROPERTY_PERMISSIONS);
    if (outputPermissions == null) {
      outputPermissions = this.permissions;
    }
    if (outputPermissions != null) {
      config.put("fs.permissions.umask-mode", FileUtils.permissionsToUmask(outputPermissions));
    }
    config.putAll(outputArguments);
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

  // Following are the set of private functions for the FileSetLocation to call
  // These are required for read/write access enforcement

  @ReadOnly
  private InputStream getInputStream(Location location) throws IOException {
    return location.getInputStream();
  }

  @WriteOnly
  private OutputStream getOutputStream(Location location) throws IOException {
    return location.getOutputStream();
  }

  @WriteOnly
  private OutputStream getOutputStream(Location location, String permission) throws IOException {
    return location.getOutputStream(permission);
  }

  @ReadOnly
  private boolean exists(Location location) throws IOException {
    return location.exists();
  }

  @WriteOnly
  private boolean createNew(Location location) throws IOException {
    return location.createNew();
  }

  @WriteOnly
  private boolean createNew(Location location, String permissions) throws IOException {
    return location.createNew(permissions);
  }

  @WriteOnly
  private boolean delete(Location location) throws IOException {
    return location.delete();
  }

  @WriteOnly
  private boolean delete(Location location, boolean recursive) throws IOException {
    return location.delete(recursive);
  }

  @WriteOnly
  private boolean mkdirs(Location location) throws IOException {
    return location.mkdirs();
  }

  @WriteOnly
  private boolean mkdirs(Location location, String permissions) throws IOException {
    return location.mkdirs(permissions);
  }

  @ReadOnly
  private long length(Location location) throws IOException {
    return location.length();
  }

  @ReadOnly
  private long lastModified(Location location) throws IOException {
    return location.lastModified();
  }

  @ReadOnly
  public boolean isDirectory(Location location) throws IOException {
    return location.isDirectory();
  }

  @ReadOnly
  private String getPermissions(Location delegate) throws IOException {
    return delegate.getPermissions();
  }

  @WriteOnly
  private void setPermissions(Location delegate, String permission) throws IOException {
    delegate.setPermissions(permission);
  }

  @WriteOnly
  private void setGroup(Location delegate, String group) throws IOException {
    delegate.setGroup(group);
  }

  @ReadOnly
  private String getGroup(Location delegate) throws IOException {
    return delegate.getGroup();
  }

  @ReadOnly
  private String getOwner(Location delegate) throws IOException {
    return delegate.getOwner();
  }

  // Note that access annotations (@ReadOnly etc.) are only enforced for methods of classes that
  // extend Dataset. Therefore we delegate all Location methods to a method of the FileDetDataset.
  // If a new method is added to Location in Apache Twill, then this code will fail to compile.
  // If FileSetLocation extended a ForwardingLocation, then this change would go unnoticed and
  // access control would not be applied to the new method.
  private final class FileSetLocation implements Location {

    private final Location delegate;
    private final LocationFactory locationFactory;

    FileSetLocation(Location delegate, LocationFactory locationFactory) {
      this.delegate = delegate;
      this.locationFactory = locationFactory;
    }

    @Override
    public URI toURI() {
      return delegate.toURI();
    }

    @Override
    public String getName() {
      return delegate.getName();
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return FileSetDataset.this.getInputStream(delegate);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      if (FileSetDataset.this.permissions != null) {
        return getOutputStream(FileSetDataset.this.permissions);
      }
      return FileSetDataset.this.getOutputStream(delegate);
    }

    @Override
    public OutputStream getOutputStream(String permission) throws IOException {
      return FileSetDataset.this.getOutputStream(delegate, permission);
    }

    @Override
    public Location append(String child) throws IOException {
      return new FileSetLocation(delegate.append(child), locationFactory);
    }

    @Override
    public Location getTempFile(String suffix) throws IOException {
      return new FileSetLocation(delegate.getTempFile(suffix), locationFactory);
    }

    @Nullable
    @Override
    public Location renameTo(Location destination) throws IOException {
      return new FileSetLocation(delegate.renameTo(getOriginal(destination)), locationFactory);
    }

    @Override
    public List<Location> list() throws IOException {
      List<Location> locations = delegate.list();
      List<Location> result = new ArrayList<>(locations.size());
      for (Location location : locations) {
        result.add(new FileSetLocation(location, locationFactory));
      }
      return result;
    }

    @Override
    public boolean exists() throws IOException {
      return FileSetDataset.this.exists(delegate);
    }

    @Override
    public boolean createNew() throws IOException {
      if (FileSetDataset.this.permissions != null) {
        return createNew(FileSetDataset.this.permissions);
      }
      return FileSetDataset.this.createNew(delegate);
    }

    @Override
    public boolean createNew(String permission) throws IOException {
      return FileSetDataset.this.createNew(delegate, permission);
    }

    @Override
    public String getPermissions() throws IOException {
      return FileSetDataset.this.getPermissions(delegate);
    }

    @Override
    public void setPermissions(String permission) throws IOException {
      FileSetDataset.this.setPermissions(delegate, permission);
    }

    @Override
    public String getOwner() throws IOException {
      return FileSetDataset.this.getOwner(delegate);
    }

    @Override
    public String getGroup() throws IOException {
      return FileSetDataset.this.getGroup(delegate);
    }

    @Override
    public void setGroup(String group) throws IOException {
      FileSetDataset.this.setGroup(delegate, group);
    }

    @Override
    public boolean delete() throws IOException {
      return FileSetDataset.this.delete(delegate);
    }

    @Override
    public boolean delete(boolean recursive) throws IOException {
      return FileSetDataset.this.delete(delegate, recursive);
    }

    @Override
    public boolean mkdirs() throws IOException {
      if (FileSetDataset.this.permissions != null) {
        return mkdirs(FileSetDataset.this.permissions);
      }
      return FileSetDataset.this.mkdirs(delegate);
    }

    @Override
    public boolean mkdirs(String permissions) throws IOException {
      return FileSetDataset.this.mkdirs(delegate, permissions);
    }

    @Override
    public long length() throws IOException {
      return FileSetDataset.this.length(delegate);
    }

    @Override
    public long lastModified() throws IOException {
      return FileSetDataset.this.lastModified(delegate);
    }

    @Override
    public boolean isDirectory() throws IOException {
      return FileSetDataset.this.isDirectory(delegate);
    }

    @Override
    public LocationFactory getLocationFactory() {
      return locationFactory;
    }

    /**
     * Finds the original {@link Location} recursively from the given location if it is a {@link FileSetLocation}.
     */
    private Location getOriginal(Location location) {
      if (!(location instanceof FileSetLocation)) {
        return location;
      }
      return getOriginal(((FileSetLocation) location).delegate);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || !(o instanceof Location)) {
        return false;
      }
      Location that = (Location) o;
      return Objects.equals(getOriginal(delegate), getOriginal(that));
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }

  private final class FileSetLocationFactory extends ForwardingLocationFactory {

    FileSetLocationFactory(LocationFactory delegate) {
      super(delegate);
    }

    @Override
    public Location create(String path) {
      return new FileSetLocation(getDelegate().create(path), this);
    }

    @Override
    public Location create(URI uri) {
      return new FileSetLocation(getDelegate().create(uri), this);
    }

    @Override
    public Location getHomeLocation() {
      return new FileSetLocation(getDelegate().getHomeLocation(), this);
    }


  }
}
