/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.tx.DatasetContext;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.internal.filesystem.LocationCodec;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionConflictException;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class manages artifacts as well as metadata for each artifact. Artifacts and their metadata cannot be changed
 * once they are written, with the exception of snapshot versions. An Artifact can contain
 * plugin classes and/or application classes. We may want to extend this to include other types of classes, such
 * as datasets.
 *
 * Every time an artifact is added, the artifact contents are stored at a location based on its id:
 * /namespaces/{namespace-id}/artifacts/{artifact-name}/{artifact-version}
 *
 * Several writes are also performed on the meta table, one for metadata about the artifact itself, and one
 * for each entity contained in the artifact.
 *
 * Artifact metadata is stored with r:{namespace}:{artifact-name} as the rowkey, {artifact-version} as the column,
 * and ArtifactMeta as the value.
 *
 * PluginClass metadata is stored with p:{namespace}:{plugin-type}:{plugin-name} as the rowkey,
 * {artifact-version}:{artifact-name} as the column, and PluginClass as the value
 *
 * TODO: (CDAP-2764) add this part when we have a better idea of what needs to be in AppClass.
 * AppClass metadata is stored with a:{namespace}:{app-name} as the rowkey,
 * {artifact-version}:{artifact-name} as the column, and AppClass as the value
 */
public class ArtifactStore {
  private static final String ARTIFACTS_PATH = "artifacts";
  private static final String ARTIFACT_PREFIX = "r:";
  private static final String PLUGIN_PREFIX = "p:";
  private static final Id.DatasetInstance META_ID =
    Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE, "artifact.meta");

  private final NamespacedLocationFactory locationFactory;
  private final Transactional<DatasetContext<Table>, Table> metaTable;
  private final Gson gson;

  @Inject
  ArtifactStore(final DatasetFramework datasetFramework,
                NamespacedLocationFactory namespacedLocationFactory,
                LocationFactory locationFactory,
                TransactionExecutorFactory txExecutorFactory) {
    this.locationFactory = namespacedLocationFactory;
    this.gson = new GsonBuilder()
      .registerTypeAdapter(Location.class, new LocationCodec(locationFactory))
      .create();
    this.metaTable = Transactional.of(txExecutorFactory, new Supplier<DatasetContext<Table>>() {
      @Override
      public DatasetContext<Table> get() {
        try {
          return DatasetContext.of((Table) DatasetsUtil.getOrCreateDataset(
            datasetFramework, META_ID, Table.class.getName(),
            DatasetProperties.builder().add(Table.PROPERTY_CONFLICT_LEVEL, ConflictDetection.COLUMN.name()).build(),
            DatasetDefinition.NO_ARGUMENTS, null));
        } catch (Exception e) {
          // there's nothing much we can do here
          throw Throwables.propagate(e);
        }
      }
    });
  }

  /**
   * Get information about all artifacts in the given namespace. If there are no artifacts in the namespace,
   * this will return an empty list. Note that existence of the namespace is not checked.
   *
   * @param namespace the namespace to get artifact information about
   * @return unmodifiable list of artifact info about every artifact in the given namespace
   * @throws IOException if there was an exception reading the artifact information from the metastore
   */
  public List<ArtifactDetail> getArtifacts(final Id.Namespace namespace) throws IOException {
    return metaTable.executeUnchecked(new TransactionExecutor.Function<DatasetContext<Table>, List<ArtifactDetail>>() {
      @Override
      public List<ArtifactDetail> apply(DatasetContext<Table> context) throws Exception {
        List<ArtifactDetail> archives = Lists.newArrayList();
        Scanner scanner = context.get().scan(scanArtifacts(namespace));
        Row row;
        while ((row = scanner.next()) != null) {
          addArchivesToList(archives, row);
        }
        return Collections.unmodifiableList(archives);
      }
    });
  }

  /**
   * Get information about all versions of the given artifact.
   *
   * @param namespace the namespace to get artifacts from
   * @param artifactName the name of the artifact to get
   * @return unmodifiable list of information about all versions of the given artifact
   * @throws ArtifactNotExistsException if no version of the given artifact exists
   * @throws IOException if there was an exception reading the artifact information from the metastore
   */
  public List<ArtifactDetail> getArtifacts(final Id.Namespace namespace, final String artifactName)
    throws ArtifactNotExistsException, IOException {

    List<ArtifactDetail> artifacts = metaTable.executeUnchecked(
      new TransactionExecutor.Function<DatasetContext<Table>, List<ArtifactDetail>>() {
        @Override
        public List<ArtifactDetail> apply(DatasetContext<Table> context) throws Exception {
          List<ArtifactDetail> archives = Lists.newArrayList();

          ArtifactKey artifactKey = new ArtifactKey(namespace, artifactName);
          Row row = context.get().get(artifactKey.getRowKey());
          if (!row.isEmpty()) {
            addArchivesToList(archives, row);
          }
          return archives;
        }
      });
    if (artifacts.isEmpty()) {
      throw new ArtifactNotExistsException(namespace, artifactName);
    }
    return Collections.unmodifiableList(artifacts);
  }

  /**
   * Get information about the given artifact.
   *
   * @param artifactId the artifact to get
   * @return information about the artifact
   * @throws ArtifactNotExistsException if the given artifact does not exist
   * @throws IOException if there was an exception reading the artifact information from the metastore
   */
  public ArtifactDetail getArtifact(final Id.Artifact artifactId) throws ArtifactNotExistsException, IOException {
    ArtifactData data = metaTable.executeUnchecked(
      new TransactionExecutor.Function<DatasetContext<Table>, ArtifactData>() {
        @Override
        public ArtifactData apply(DatasetContext<Table> context) throws Exception {
          ArtifactCell artifactCell = new ArtifactCell(artifactId);
          byte[] value = context.get().get(artifactCell.rowkey, artifactCell.column);
          return value == null ? null : gson.fromJson(Bytes.toString(value), ArtifactData.class);
        }
      });

    if (data == null) {
      throw new ArtifactNotExistsException(artifactId);
    }
    return new ArtifactDetail(new ArtifactInfo(artifactId, data.location), data.meta);
  }

  /**
   * Get all plugin classes in a given namespace. Results are returned as a map from artifact to plugins
   * in that artifact.
   *
   * @param namespace the namespace to look for plugins in
   * @return unmodifiable map of artifact info to plugin classes in the namespace.
   *         The map will never be null. If there are no plugin classes, an empty map will be returned.
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public Map<ArtifactInfo, List<PluginClass>> getPluginClasses(final Id.Namespace namespace) throws IOException {
    return metaTable.executeUnchecked(
      new TransactionExecutor.Function<DatasetContext<Table>, Map<ArtifactInfo, List<PluginClass>>>() {
        @Override
        public Map<ArtifactInfo, List<PluginClass>> apply(DatasetContext<Table> context) throws Exception {
          Map<ArtifactInfo, List<PluginClass>> result = Maps.newHashMap();

          Scanner scanner = context.get().scan(scanPlugins(namespace));
          Row row;
          while ((row = scanner.next()) != null) {
            addPluginsToMap(result, row);
          }
          return Collections.unmodifiableMap(result);
        }
      });
  }

  /**
   * Get all plugin classes of a specific type in the given namespace.
   * Results are returned as a map from artifact to plugins in that artifact.
   *
   * @param namespace the namespace to look for plugins in
   * @param type the type of plugin to look for
   * @return a map of artifact info to plugin classes for all plugin classes of the given type in the namespace.
   *         The map will never be null. If there are no plugin classes, an empty map will be returned.
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public Map<ArtifactInfo, List<PluginClass>> getPluginClasses(final Id.Namespace namespace,
                                                               final String type) throws IOException {
    return metaTable.executeUnchecked(
      new TransactionExecutor.Function<DatasetContext<Table>, Map<ArtifactInfo, List<PluginClass>>>() {
        @Override
        public Map<ArtifactInfo, List<PluginClass>> apply(DatasetContext<Table> context) throws Exception {
          Map<ArtifactInfo, List<PluginClass>> result = Maps.newHashMap();

          Scanner scanner = context.get().scan(scanPlugins(namespace, type));
          Row row;
          while ((row = scanner.next()) != null) {
            addPluginsToMap(result, row);
          }
          return result;
        }
      });
  }

  /**
   * Get all plugin classes of a specific type and name in the given namespace.
   * Results are returned as a map from artifact to plugins in that artifact.
   *
   * @param namespace the namespace to look for plugins in
   * @param type the type of plugin to look for
   * @param name the name of the plugin to look for
   * @return a map of artifact info to plugin classes of the given type and name in the namespace.
   *         The map will never be null, and will never be empty.
   * @throws PluginNotExistsException if no plugin with the given type and name exists in the namespace
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public Map<ArtifactInfo, List<PluginClass>> getPluginClasses(
    final Id.Namespace namespace, final String type, final String name) throws IOException, PluginNotExistsException {

    Map<ArtifactInfo, List<PluginClass>> plugins = metaTable.executeUnchecked(
      new TransactionExecutor.Function<DatasetContext<Table>, Map<ArtifactInfo, List<PluginClass>>>() {
        @Override
        public Map<ArtifactInfo, List<PluginClass>> apply(DatasetContext<Table> context) throws Exception {
          Map<ArtifactInfo, List<PluginClass>> result = Maps.newHashMap();

          PluginKey pluginKey = new PluginKey(namespace, type, name);
          Row row = context.get().get(pluginKey.getRowKey());
          if (!row.isEmpty()) {
            addPluginsToMap(result, row);
          }
          return result;
        }
      });
    if (plugins.isEmpty()) {
      throw new PluginNotExistsException(namespace, type, name);
    }
    return plugins;
  }

  /**
   * Get the plugin class for a specific plugin type, name, and belonging to a specific artifact.
   * Results are returned as a map from artifact to plugins in that artifact.
   *
   * @param artifactId the id of the artifact the plugin is from
   * @param type the type of plugin to look for
   * @param name the name of the plugin to look for
   * @return a map entry containing the artifact info and plugin class details. Never null
   * @throws PluginNotExistsException if no such plugin exists
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public Map.Entry<ArtifactInfo, PluginClass> getPluginClass(
    final Id.Artifact artifactId, final String type, final String name) throws IOException, PluginNotExistsException {
    Map.Entry<ArtifactInfo, PluginClass> plugin = metaTable.executeUnchecked(
      new TransactionExecutor.Function<DatasetContext<Table>, Map.Entry<ArtifactInfo, PluginClass>>() {
        @Override
        public Map.Entry<ArtifactInfo, PluginClass> apply(DatasetContext<Table> context) throws Exception {
          PluginKey pluginKey = new PluginKey(artifactId.getNamespace(), type, name);
          ArtifactColumn column = new ArtifactColumn(artifactId.getVersion().getVersion(), artifactId.getName());
          byte[] value = context.get().get(pluginKey.getRowKey(), column.getColumn());
          if (value == null) {
            return null;
          }
          PluginData pluginData = gson.fromJson(Bytes.toString(value), PluginData.class);
          return Maps.immutableEntry(new ArtifactInfo(artifactId, pluginData.artifactLocation), pluginData.pluginClass);
        }
      });
    if (plugin == null) {
      throw new PluginNotExistsException(artifactId, type, name);
    }
    return plugin;
  }

  /**
   * Write the artifact and its metadata to the store. Once added, artifacts cannot be changed unless they are
   * snapshot versions.
   *
   * @param artifactId the id of the artifact to add
   * @param artifactMeta the metadata for the artifact
   * @param artifactContents the contents of the artifact
   * @throws WriteConflictException if the artifact is already currently being written
   * @throws ArtifactAlreadyExistsException if a non-snapshot version of the artifact already exists
   * @throws IOException if there was an exception persisting the artifact contents to the filesystem,
   *                     of persisting the artifact metadata to the metastore
   */
  public void write(final Id.Artifact artifactId, final ArtifactMeta artifactMeta, final InputStream artifactContents)
    throws WriteConflictException, ArtifactAlreadyExistsException, IOException {

    Location fileDirectory =
      locationFactory.get(artifactId.getNamespace(), ARTIFACTS_PATH).append(artifactId.getName());
    Locations.mkdirsIfNotExists(fileDirectory);

    // write the file contents
    final Location destination = fileDirectory.append(artifactId.getVersion().getVersion()).getTempFile(".jar");
    ByteStreams.copy(artifactContents, destination.getOutputStream());

    // now try and write the metadata for the artifact
    try {
      boolean written = metaTable.execute(new TransactionExecutor.Function<DatasetContext<Table>, Boolean>() {

        @Override
        public Boolean apply(DatasetContext<Table> context) throws Exception {
          Table table = context.get();

          ArtifactCell artifactCell = new ArtifactCell(artifactId);
          byte[] existingMetaBytes = table.get(artifactCell.rowkey, artifactCell.column);
          boolean isSnapshot = artifactId.getVersion().isSnapshot();
          if (existingMetaBytes != null && !isSnapshot) {
            // non-snapshot artifacts are immutable. If there is existing metadata, stop here.
            return false;
          }

          // write artifact metadata
          ArtifactData data = new ArtifactData(destination, artifactMeta);
          writeMeta(table, artifactId, data);
          if (existingMetaBytes != null) {
            cleanupOldSnapshot(table, artifactId, existingMetaBytes, data);
          }
          return true;
        }
      });

      if (!written) {
        throw new ArtifactAlreadyExistsException(artifactId);
      }
    } catch (TransactionConflictException e) {
      destination.delete();
      throw new WriteConflictException(artifactId);
    } catch (TransactionFailureException | InterruptedException e) {
      destination.delete();
      throw new IOException(e);
    }
  }

  /**
   * Clear all data in the given namespace. Not terribly efficient, can do more deletes than needed.
   * This is purely for unit tests, so it's not a big concern.
   *
   * @param namespace the namespace to delete data in
   * @throws IOException if there was some problem deleting the data
   */
  @VisibleForTesting
  void clear(Id.Namespace namespace) throws IOException {
    locationFactory.get(namespace, ARTIFACTS_PATH).delete();
    for (final ArtifactDetail artifactDetail : getArtifacts(namespace)) {

      metaTable.executeUnchecked(new TransactionExecutor.Function<DatasetContext<Table>, Void>() {
        @Override
        public Void apply(DatasetContext<Table> context) throws Exception {
          final Id.Artifact artifactId = artifactDetail.getInfo().getId();
          ArtifactKey artifactKey = new ArtifactKey(artifactId.getNamespace(), artifactId.getName());
          context.get().delete(artifactKey.getRowKey());

          for (PluginClass pluginClass : artifactDetail.getMeta().getPlugins()) {
            PluginKey pluginKey =
              new PluginKey(artifactId.getNamespace(), pluginClass.getType(), pluginClass.getName());
            context.get().delete(pluginKey.getRowKey());
          }
          return null;
        }
      });
    }
  }

  // write a new artifact snapshot and clean up the old snapshot data
  private void writeMeta(Table table, Id.Artifact artifactId, ArtifactData data) throws IOException {
    ArtifactCell artifactCell = new ArtifactCell(artifactId);
    table.put(artifactCell.rowkey, artifactCell.column, Bytes.toBytes(gson.toJson(data)));

    // column for plugin meta and app meta. {artifact-name}:{artifact-version}
    // does not need to contain namespace because namespace is in the rowkey
    ArtifactColumn artifactColumn =
      new ArtifactColumn(artifactId.getVersion().getVersion(), artifactId.getName());

    // write pluginClass metadata
    for (PluginClass pluginClass : data.meta.getPlugins()) {
      // p:{namespace}:{type}:{name}
      PluginKey pluginKey =
        new PluginKey(artifactId.getNamespace(), pluginClass.getType(), pluginClass.getName());
      byte[] pluginDataBytes = Bytes.toBytes(gson.toJson(new PluginData(pluginClass, data.location)));
      table.put(pluginKey.getRowKey(), artifactColumn.getColumn(), pluginDataBytes);
    }

    // TODO: write appClass metadata
  }

  // if we are overwriting a previous snapshot, need to clean up the old snapshot data
  // this means cleaning up the old jar, and performing a diff of the metadata to remove plugins
  // that used to be in the old jar but are not in the current jar
  private void cleanupOldSnapshot(Table table, Id.Artifact artifactId,
                                  byte[] oldData, ArtifactData newData) throws IOException {
    ArtifactData oldMeta = gson.fromJson(Bytes.toString(oldData), ArtifactData.class);

    // delete old plugins that were removed
    Set<PluginClass> oldPlugins = Sets.newHashSet(oldMeta.meta.getPlugins());
    Set<PluginClass> currentPlugins = Sets.newHashSet(newData.meta.getPlugins());
    Set<PluginClass> pluginsToRemove = Sets.difference(oldPlugins, currentPlugins);
    ArtifactColumn artifactColumn = new ArtifactColumn(artifactId.getVersion().getVersion(), artifactId.getName());
    for (PluginClass pluginClass : pluginsToRemove) {
      PluginKey pluginKey = new PluginKey(artifactId.getNamespace(), pluginClass.getType(), pluginClass.getName());
      table.delete(pluginKey.getRowKey(), artifactColumn.getColumn());
    }

    // delete the old jar file
    oldMeta.location.delete();
  }

  private void addArchivesToList(List<ArtifactDetail> archives, Row row) throws IOException {
    ArtifactKey artifactKey = ArtifactKey.parse(row.getRow());

    for (Map.Entry<byte[], byte[]> columnVal : row.getColumns().entrySet()) {
      String version = Bytes.toString(columnVal.getKey());
      ArtifactData data = gson.fromJson(Bytes.toString(columnVal.getValue()), ArtifactData.class);
      Id.Artifact artifactId = Id.Artifact.from(artifactKey.namespace, artifactKey.name, version);
      archives.add(new ArtifactDetail(new ArtifactInfo(artifactId, data.location), data.meta));
    }
  }

  // given a row representing plugin metadata, add all plugins in the row to the given map
  private void addPluginsToMap(Map<ArtifactInfo, List<PluginClass>> map, Row row) throws IOException {
    // plugin key contains namespace, plugin type, and plugin name
    PluginKey pluginKey = PluginKey.parse(row.getRow());

    // column is the artifact name and version, value is the serialized PluginClass
    for (Map.Entry<byte[], byte[]> column : row.getColumns().entrySet()) {
      ArtifactColumn artifactColumn = ArtifactColumn.parse(column.getKey());
      PluginData pluginData = gson.fromJson(Bytes.toString(column.getValue()), PluginData.class);

      Id.Artifact artifactId =
        Id.Artifact.from(pluginKey.namespace, artifactColumn.name, artifactColumn.version);
      ArtifactInfo artifactInfo = new ArtifactInfo(artifactId, pluginData.artifactLocation);

      if (!map.containsKey(artifactInfo)) {
        map.put(artifactInfo, Lists.<PluginClass>newArrayList());
      }
      map.get(artifactInfo).add(pluginData.pluginClass);
    }
  }

  private Scan scanArtifacts(Id.Namespace namespace) {
    return new Scan(
      Bytes.toBytes(String.format("%s%s:", ARTIFACT_PREFIX, namespace.getId())),
      Bytes.toBytes(String.format("%s%s;", ARTIFACT_PREFIX, namespace.getId())));
  }

  private Scan scanPlugins(Id.Namespace namespace) {
    return new Scan(
      Bytes.toBytes(String.format("%s%s:", PLUGIN_PREFIX, namespace.getId())),
      Bytes.toBytes(String.format("%s%s;", PLUGIN_PREFIX, namespace.getId())));
  }

  private Scan scanPlugins(Id.Namespace namespace, String type) {
    return new Scan(
      Bytes.toBytes(String.format("%s%s:%s:", PLUGIN_PREFIX, namespace.getId(), type)),
      Bytes.toBytes(String.format("%s%s:%s;", PLUGIN_PREFIX, namespace.getId(), type)));
  }

  private static class PluginKey {
    private final Id.Namespace namespace;
    private final String type;
    private final String name;

    private PluginKey(Id.Namespace namespace, String type, String name) {
      this.namespace = namespace;
      this.type = type;
      this.name = name;
    }

    private byte[] getRowKey() {
      return Bytes.toBytes(String.format("%s%s:%s:%s", PLUGIN_PREFIX, namespace.getId(), type, name));
    }

    private static PluginKey parse(byte[] rowkey) {
      String key = Bytes.toString(rowkey);
      int namespaceEnd = key.indexOf(':', PLUGIN_PREFIX.length());
      Id.Namespace namespace = Id.Namespace.from(key.substring(PLUGIN_PREFIX.length(), namespaceEnd));
      int typeEnd = key.indexOf(':', namespaceEnd + 1);
      String type = key.substring(namespaceEnd + 1, typeEnd);
      String name = key.substring(typeEnd + 1);
      return new PluginKey(namespace, type, name);
    }
  }

  private static class ArtifactColumn {
    private final String version;
    private final String name;

    private ArtifactColumn(String version, String name) {
      this.version = version;
      this.name = name;
    }

    private byte[] getColumn() {
      return Bytes.toBytes(String.format("%s:%s", version, name));
    }

    private static ArtifactColumn parse(byte[] columnBytes) {
      String columnStr = Bytes.toString(columnBytes);
      int separatorIndex = columnStr.indexOf(':');
      return new ArtifactColumn(columnStr.substring(0, separatorIndex), columnStr.substring(separatorIndex + 1));
    }
  }

  // utilities for creating and parsing row keys for artifacts. Keys are of the form 'r:{namespace}:{artifact-name}'
  private static class ArtifactKey {
    private final Id.Namespace namespace;
    private final String name;

    private ArtifactKey(Id.Namespace namespace, String name) {
      this.namespace = namespace;
      this.name = name;
    }

    private byte[] getRowKey() {
      return Bytes.toBytes(String.format("%s%s:%s", ARTIFACT_PREFIX, namespace.getId(), name));
    }

    private static ArtifactKey parse(byte[] rowkey) {
      String key = Bytes.toString(rowkey);
      int separatorIdx = key.indexOf(':', ARTIFACT_PREFIX.length());
      return new ArtifactKey(Id.Namespace.from(key.substring(ARTIFACT_PREFIX.length(), separatorIdx)),
        key.substring(separatorIdx + 1));
    }
  }

  private static class ArtifactCell {
    private final byte[] rowkey;
    private final byte[] column;

    private ArtifactCell(Id.Artifact artifactId) {
      rowkey = new ArtifactKey(artifactId.getNamespace(), artifactId.getName()).getRowKey();
      column = Bytes.toBytes(artifactId.getVersion().getVersion());
    }
  }

  // Data that will be stored for an artifact. Same as ArtifactDetail, expected without the id since that is redundant.
  private static class ArtifactData {
    private final Location location;
    private final ArtifactMeta meta;

    public ArtifactData(Location location, ArtifactMeta meta) {
      this.location = location;
      this.meta = meta;
    }
  }

  // Data that will be stored for a plugin.
  private static class PluginData {
    private final PluginClass pluginClass;
    private final Location artifactLocation;

    public PluginData(PluginClass pluginClass, Location artifactLocation) {
      this.pluginClass = pluginClass;
      this.artifactLocation = artifactLocation;
    }
  }
}
