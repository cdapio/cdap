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
import co.cask.cdap.api.data.schema.Schema;
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
import co.cask.cdap.internal.artifact.ArtifactVersion;
import co.cask.cdap.internal.filesystem.LocationCodec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionConflictException;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * This class manages artifacts as well as metadata for each artifact. Artifacts and their metadata cannot be changed
 * once they are written, with the exception of snapshot versions. An Artifact can contain
 * plugin classes and/or application classes. We may want to extend this to include other types of classes, such
 * as datasets.
 *
 * Every time an artifact is added, the artifact contents are stored at a base location based on its id:
 * /namespaces/{namespace-id}/artifacts/{artifact-name}/{artifact-version}
 * Several metadata writes are then performed.
 *
 * The first adds metadata about the artifact, with
 * rowkey r:{namespace}:{artifact-name}, column {artifact-version}, and ArtifactDetail as the value
 *
 * TODO: (CDAP-2764) add this part when we have a better idea of what needs to be in AppClass.
 * The second adds metadata about any Application Class contained in the artifact, with
 * rowkey a:{namespace}:{classname}, column {artifact-name}:{artifact-version}, and AppClass as the value
 *
 * The third adds metadata about any Plugin contained in the artifact, with
 * rowkey p:{parent-namespace}:{parent-name}:{plugin-type}:{plugin-name},
 * column {artifact-namespace}:{artifact-name}:{artifact-version},
 * and PluginDetailCodec as the value
 *
 * For example, suppose we add a system artifact etlbatch-3.1.0, which contains an ETLBatch application class.
 * The meta table will look like:
 *
 * rowkey                            columns
 * a:system:ETLBatch                 etlbatch:3.1.0 -> {AppData}
 * r:system:etlbatch                 3.1.0 -> {ArtifactData}
 *
 * After that, a system artifact etlbatch-lib-3.1.0 is added, which extends etlbatch and contains
 * stream sink and table sink plugins. The meta table will look like:
 *
 * rowkey                            columns
 * a:system:ETLBatch                 etlbatch:3.1.0 -> {AppData}
 * p:system:etlbatch:sink:stream     system:etlbatch-lib:3.1.0 -> {PluginData}
 * p:system:etlbatch:sink:table      system:etlbatch-lib:3.1.0 -> {PluginData}
 * r:system:etlbatch                 3.1.0 -> {ArtifactData}
 * r:system:etlbatch-lib             3.1.0 -> {ArtifactData}
 *
 * Finally a user adds artifact custom-sources-1.0.0 to the default namespace,
 * which extends etlbatch and contains a db source plugin. The meta table will look like:
 *
 * rowkey                            columns
 * a:system:ETLBatch                 etlbatch:3.1.0 -> {AppData}
 * p:system:etlbatch:sink:stream     system:etlbatch-lib:3.1.0 -> {PluginData}
 * p:system:etlbatch:sink:table      system:etlbatch-lib:3.1.0 -> {PluginData}
 * p:system:etlbatch:source:db       default:custom-sources:1.0.0 -> {PluginData}
 * r:default:custom-sources          1.0.0 -> {ArtifactData}
 * r:system:etlbatch                 3.1.0 -> {ArtifactData}
 * r:system:etlbatch-lib             3.1.0 -> {ArtifactData}
 *
 * With this schema we can perform a scan to look up AppClasses, a scan to look up plugins that extend a specific
 * artifact, and a scan to look up artifacts.
 */
public class ArtifactStore {
  private static final String ARTIFACTS_PATH = "artifacts";
  private static final String ARTIFACT_PREFIX = "r";
  private static final String PLUGIN_PREFIX = "p";
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
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
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
        List<ArtifactDetail> artifacts = Lists.newArrayList();
        Scanner scanner = context.get().scan(scanArtifacts(namespace));
        Row row;
        while ((row = scanner.next()) != null) {
          addArtifactsToList(artifacts, row);
        }
        return Collections.unmodifiableList(artifacts);
      }
    });
  }

  /**
   * Get all artifacts that match artifacts in the given ranges.
   *
   * @param range the range to match artifacts in
   * @return an unmodifiable list of all artifacts that match the given ranges. If none exist, an empty list
   *         is returned
   */
  public List<ArtifactDetail> getArtifacts(final ArtifactRange range) {
    return metaTable.executeUnchecked(new TransactionExecutor.Function<DatasetContext<Table>, List<ArtifactDetail>>() {
      @Override
      public List<ArtifactDetail> apply(DatasetContext<Table> context) throws Exception {
        List<ArtifactDetail> artifacts = Lists.newArrayList();
        Table table = context.get();
        ArtifactKey artifactKey = new ArtifactKey(range.getNamespace(), range.getName());

        Row row = table.get(artifactKey.getRowKey());
        for (Map.Entry<byte[], byte[]> columnEntry : row.getColumns().entrySet()) {
          String version = Bytes.toString(columnEntry.getKey());
          if (range.versionIsInRange(new ArtifactVersion(version))) {
            ArtifactData data = gson.fromJson(Bytes.toString(columnEntry.getValue()), ArtifactData.class);
            Id.Artifact artifactId = Id.Artifact.from(artifactKey.namespace, artifactKey.name, version);
            artifacts.add(new ArtifactDetail(getDescriptor(artifactId, data.location), data.meta));
          }
        }
        return Collections.unmodifiableList(artifacts);
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
            addArtifactsToList(archives, row);
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
    return new ArtifactDetail(getDescriptor(artifactId, data.location), data.meta);
  }

  /**
   * Get all plugin classes that extend the given parent artifact.
   * Results are returned as a sorted map from plugin artifact to plugins in that artifact.
   * Map entries are sorted by the artifact
   *
   * @param parentArtifactId the id of the artifact to find plugins for
   * @return an unmodifiable map of plugin artifact to plugin classes for all plugin classes accessible by the given
   *         artifact. The map will never be null. If there are no plugin classes, an empty map will be returned.
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public SortedMap<ArtifactDescriptor, List<PluginClass>> getPluginClasses(final Id.Artifact parentArtifactId)
    throws IOException {

    return metaTable.executeUnchecked(
      new TransactionExecutor.Function<DatasetContext<Table>, SortedMap<ArtifactDescriptor, List<PluginClass>>>() {
        @Override
        public SortedMap<ArtifactDescriptor, List<PluginClass>> apply(DatasetContext<Table> context) throws Exception {
          SortedMap<ArtifactDescriptor, List<PluginClass>> result = Maps.newTreeMap();

          Scanner scanner = context.get().scan(scanPlugins(parentArtifactId));
          Row row;
          while ((row = scanner.next()) != null) {
            addPluginsToMap(parentArtifactId, result, row);
          }
          return Collections.unmodifiableSortedMap(result);
        }
      });
  }

  /**
   * Get all plugin classes of the given type that extend the given parent artifact.
   * Results are returned as a map from plugin artifact to plugins in that artifact.
   *
   * @param parentArtifactId the id of the artifact to find plugins for
   * @param type the type of plugin to look for
   * @return an unmodifiable map of plugin artifact to plugin classes for all plugin classes accessible by the
   *         given artifact. The map will never be null. If there are no plugin classes, an empty map will be returned.
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public SortedMap<ArtifactDescriptor, List<PluginClass>> getPluginClasses(final Id.Artifact parentArtifactId,
                                                                           final String type) throws IOException {
    return metaTable.executeUnchecked(
      new TransactionExecutor.Function<DatasetContext<Table>, SortedMap<ArtifactDescriptor, List<PluginClass>>>() {
        @Override
        public SortedMap<ArtifactDescriptor, List<PluginClass>> apply(DatasetContext<Table> context) throws Exception {
          SortedMap<ArtifactDescriptor, List<PluginClass>> result = Maps.newTreeMap();

          Scanner scanner = context.get().scan(scanPlugins(parentArtifactId, type));
          Row row;
          while ((row = scanner.next()) != null) {
            addPluginsToMap(parentArtifactId, result, row);
          }
          return Collections.unmodifiableSortedMap(result);
        }
      });
  }

  /**
   * Get all plugin classes of the given type and name that extend the given parent artifact.
   * Results are returned as a map from plugin artifact to plugins in that artifact.
   *
   * @param parentArtifactId the id of the artifact to find plugins for
   * @param type the type of plugin to look for
   * @param name the name of the plugin to look for
   * @return an unmodifiable map of plugin artifact to plugin classes of the given type and name, accessible by the
   *         given artifact. The map will never be null, and will never be empty.
   * @throws PluginNotExistsException if no plugin with the given type and name exists in the namespace
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public SortedMap<ArtifactDescriptor, PluginClass> getPluginClasses(final Id.Artifact parentArtifactId,
                                                                     final String type, final String name)
    throws IOException, PluginNotExistsException {

    SortedMap<ArtifactDescriptor, PluginClass> plugins = metaTable.executeUnchecked(
      new TransactionExecutor.Function<DatasetContext<Table>, SortedMap<ArtifactDescriptor, PluginClass>>() {
        @Override
        public SortedMap<ArtifactDescriptor, PluginClass> apply(DatasetContext<Table> context) throws Exception {
          SortedMap<ArtifactDescriptor, PluginClass> result = Maps.newTreeMap();

          PluginKey pluginKey = new PluginKey(parentArtifactId.getNamespace(), parentArtifactId.getName(), type, name);
          Row row = context.get().get(pluginKey.getRowKey());
          if (!row.isEmpty()) {
            // column is the artifact name and version, value is the serialized PluginClass
            for (Map.Entry<byte[], byte[]> column : row.getColumns().entrySet()) {
              ArtifactColumn artifactColumn = ArtifactColumn.parse(column.getKey());
              PluginData pluginData = gson.fromJson(Bytes.toString(column.getValue()), PluginData.class);
              // filter out plugins that don't extend this version of the parent artifact
              if (matches(pluginData, parentArtifactId.getVersion())) {
                ArtifactDescriptor artifactInfo = getDescriptor(artifactColumn.artifactId, pluginData.artifactLocation);
                result.put(artifactInfo, pluginData.pluginClass);
              }
            }
          }
          return result;
        }
      });
    if (plugins.isEmpty()) {
      throw new PluginNotExistsException(parentArtifactId.getNamespace(), type, name);
    }
    return Collections.unmodifiableSortedMap(plugins);
  }

  private boolean matches(PluginData pluginData, ArtifactVersion version) {
    ArtifactVersion lower = new ArtifactVersion(pluginData.parentVersionLower);
    ArtifactVersion upper = new ArtifactVersion(pluginData.parentVersionUpper);
    return version.compareTo(lower) >= 0 && version.compareTo(upper) < 0;
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

    // if we're not a snapshot version, check that the artifact doesn't exist already.
    final ArtifactCell artifactCell = new ArtifactCell(artifactId);
    if (!artifactId.getVersion().isSnapshot()) {
      byte[] existingMeta = metaTable.executeUnchecked(
        new TransactionExecutor.Function<DatasetContext<Table>, byte[]>() {
          @Override
          public byte[] apply(DatasetContext<Table> context) throws Exception {
            Table table = context.get();
            return table.get(artifactCell.rowkey, artifactCell.column);
          }
        });

      if (existingMeta != null) {
        throw new ArtifactAlreadyExistsException(artifactId);
      }
    }

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

          // we have to check that the metadata doesn't exist again since somebody else may have written
          // the artifact while we were copying the artifact to the filesystem.
          byte[] existingMetaBytes = table.get(artifactCell.rowkey, artifactCell.column);
          boolean isSnapshot = artifactId.getVersion().isSnapshot();
          if (existingMetaBytes != null && !isSnapshot) {
            // non-snapshot artifacts are immutable. If there is existing metadata, stop here.
            return false;
          }

          ArtifactData data = new ArtifactData(destination, artifactMeta);
          // cleanup existing metadata if it exists and this is a snapshot
          if (existingMetaBytes != null) {
            cleanupOldSnapshot(table, artifactId, existingMetaBytes);
          }
          // write artifact metadata
          writeMeta(table, artifactId, data);
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
   * Clear all data in the given namespace. Used only in unit tests.
   *
   * @param namespace the namespace to delete data in
   * @throws IOException if there was some problem deleting the data
   */
  @VisibleForTesting
  void clear(final Id.Namespace namespace) throws IOException {
    locationFactory.get(namespace, ARTIFACTS_PATH).delete();

    metaTable.executeUnchecked(new TransactionExecutor.Function<DatasetContext<Table>, Void>() {
      @Override
      public Void apply(DatasetContext<Table> context) throws Exception {
        Table table = context.get();
        Scanner scanner = table.scan(scanArtifacts(namespace));
        Row row;
        while ((row = scanner.next()) != null) {
          table.delete(row.getRow());
        }
        Scan pluginsScan = new Scan(
          Bytes.toBytes(String.format("%s:%s:", PLUGIN_PREFIX, namespace.getId())),
          Bytes.toBytes(String.format("%s:%s;", PLUGIN_PREFIX, namespace.getId()))
        );
        scanner = table.scan(pluginsScan);
        while ((row = scanner.next()) != null) {
          table.delete(row.getRow());
        }
        return null;
      }
    });
  }

  // write a new artifact snapshot and clean up the old snapshot data
  private void writeMeta(Table table, Id.Artifact artifactId, ArtifactData data) throws IOException {
    ArtifactCell artifactCell = new ArtifactCell(artifactId);
    table.put(artifactCell.rowkey, artifactCell.column, Bytes.toBytes(gson.toJson(data)));

    // column for plugin meta and app meta. {artifact-name}:{artifact-version}
    // does not need to contain namespace because namespace is in the rowkey
    ArtifactColumn artifactColumn = new ArtifactColumn(artifactId);

    // write pluginClass metadata
    for (PluginClass pluginClass : data.meta.getClasses().getPlugins()) {
      // write metadata for each artifact this plugin extends
      for (ArtifactRange artifactRange : data.meta.getUsableBy()) {
        // p:{namespace}:{type}:{name}
        PluginKey pluginKey = new PluginKey(
          artifactRange.getNamespace(), artifactRange.getName(), pluginClass.getType(), pluginClass.getName());

        byte[] pluginDataBytes = Bytes.toBytes(gson.toJson(new PluginData(pluginClass, artifactRange, data.location)));
        table.put(pluginKey.getRowKey(), artifactColumn.getColumn(), pluginDataBytes);
      }
    }

    // TODO: write appClass metadata
  }

  // if we are overwriting a previous snapshot, need to clean up the old snapshot data
  // this means cleaning up the old jar, and deleting plugin and app rows.
  private void cleanupOldSnapshot(Table table, Id.Artifact artifactId, byte[] oldData) throws IOException {
    // delete old artifact data
    ArtifactCell artifactCell = new ArtifactCell(artifactId);
    table.delete(artifactCell.rowkey, artifactCell.column);

    // delete old plugins
    ArtifactData oldMeta = gson.fromJson(Bytes.toString(oldData), ArtifactData.class);
    ArtifactColumn artifactColumn = new ArtifactColumn(artifactId);

    for (PluginClass pluginClass : oldMeta.meta.getClasses().getPlugins()) {
      // write metadata for each artifact this plugin extends
      for (ArtifactRange artifactRange : oldMeta.meta.getUsableBy()) {
        // p:{namespace}:{type}:{name}
        PluginKey pluginKey = new PluginKey(
          artifactRange.getNamespace(), artifactRange.getName(), pluginClass.getType(), pluginClass.getName());
        table.delete(pluginKey.getRowKey(), artifactColumn.getColumn());
      }
    }

    // TODO: delete appClass metadata

    // delete the old jar file
    oldMeta.location.delete();
  }

  private void addArtifactsToList(List<ArtifactDetail> artifactDetails, Row row) throws IOException {
    ArtifactKey artifactKey = ArtifactKey.parse(row.getRow());

    for (Map.Entry<byte[], byte[]> columnVal : row.getColumns().entrySet()) {
      String version = Bytes.toString(columnVal.getKey());
      ArtifactData data = gson.fromJson(Bytes.toString(columnVal.getValue()), ArtifactData.class);
      Id.Artifact artifactId = Id.Artifact.from(artifactKey.namespace, artifactKey.name, version);
      artifactDetails.add(new ArtifactDetail(getDescriptor(artifactId, data.location), data.meta));
    }
  }

  // this method examines all plugins in the given row and checks if they extend the given parent artifact.
  // if so, information about the plugin artifact and the plugin details are added to the given map.
  private void addPluginsToMap(Id.Artifact parentArtifactId, SortedMap<ArtifactDescriptor, List<PluginClass>> map,
                               Row row) throws IOException {
    // column is the artifact namespace, name, and version. value is the serialized PluginData
    for (Map.Entry<byte[], byte[]> column : row.getColumns().entrySet()) {
      ArtifactColumn artifactColumn = ArtifactColumn.parse(column.getKey());
      PluginData pluginData = gson.fromJson(Bytes.toString(column.getValue()), PluginData.class);

      // filter out plugins that don't extend this version of the parent artifact
      if (matches(pluginData, parentArtifactId.getVersion())) {
        ArtifactDescriptor artifactInfo = getDescriptor(artifactColumn.artifactId, pluginData.artifactLocation);

        if (!map.containsKey(artifactInfo)) {
          map.put(artifactInfo, Lists.<PluginClass>newArrayList());
        }
        map.get(artifactInfo).add(pluginData.pluginClass);
      }
    }
  }

  private ArtifactDescriptor getDescriptor(Id.Artifact artifactId, Location location) {
    return new ArtifactDescriptor(artifactId.getName(), artifactId.getVersion(),
                                  Constants.SYSTEM_NAMESPACE_ID.equals(artifactId.getNamespace()), location);
  }

  private Scan scanArtifacts(Id.Namespace namespace) {
    return new Scan(
      Bytes.toBytes(String.format("%s:%s:", ARTIFACT_PREFIX, namespace.getId())),
      Bytes.toBytes(String.format("%s:%s;", ARTIFACT_PREFIX, namespace.getId())));
  }

  private Scan scanPlugins(Id.Artifact parentArtifactId) {
    return new Scan(
      Bytes.toBytes(String.format("%s:%s:%s:",
        PLUGIN_PREFIX, parentArtifactId.getNamespace().getId(), parentArtifactId.getName())),
      Bytes.toBytes(String.format("%s:%s:%s;",
        PLUGIN_PREFIX, parentArtifactId.getNamespace().getId(), parentArtifactId.getName())));
  }

  private Scan scanPlugins(Id.Artifact parentArtifactId, String type) {
    return new Scan(
      Bytes.toBytes(String.format("%s:%s:%s:%s:",
        PLUGIN_PREFIX, parentArtifactId.getNamespace().getId(), parentArtifactId.getName(), type)),
      Bytes.toBytes(String.format("%s:%s:%s:%s;",
        PLUGIN_PREFIX, parentArtifactId.getNamespace().getId(), parentArtifactId.getName(), type)));
  }

  private static class PluginKey {
    private final Id.Namespace parentArtifactNamespace;
    private final String parentArtifactName;
    private final String type;
    private final String name;

    private PluginKey(Id.Namespace parentArtifactNamespace, String parentArtifactName, String type, String name) {
      this.parentArtifactNamespace = parentArtifactNamespace;
      this.parentArtifactName = parentArtifactName;
      this.type = type;
      this.name = name;
    }

    // p:system:etlbatch:sink:table
    private byte[] getRowKey() {
      return Bytes.toBytes(
        Joiner.on(':').join(PLUGIN_PREFIX, parentArtifactNamespace.getId(), parentArtifactName, type, name));
    }
  }

  private static class ArtifactColumn {
    private final Id.Artifact artifactId;

    private ArtifactColumn(Id.Artifact artifactId) {
      this.artifactId = artifactId;
    }

    private byte[] getColumn() {
      return Bytes.toBytes(String.format("%s:%s:%s",
        artifactId.getNamespace().getId(), artifactId.getName(), artifactId.getVersion().getVersion()));
    }

    private static ArtifactColumn parse(byte[] columnBytes) {
      String columnStr = Bytes.toString(columnBytes);
      Iterator<String> parts = Splitter.on(':').limit(3).split(columnStr).iterator();
      Id.Namespace namespace = Id.Namespace.from(parts.next());
      return new ArtifactColumn(Id.Artifact.from(namespace, parts.next(), parts.next()));
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
      return Bytes.toBytes(Joiner.on(':').join(ARTIFACT_PREFIX, namespace.getId(), name));
    }

    private static ArtifactKey parse(byte[] rowkey) {
      String key = Bytes.toString(rowkey);
      Iterator<String> parts = Splitter.on(':').limit(4).split(key).iterator();
      // first part is the artifact prefix
      parts.next();
      // next is namespace, then name
      return new ArtifactKey(Id.Namespace.from(parts.next()), parts.next());
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
    private final String parentVersionLower;
    private final String parentVersionUpper;
    private final Location artifactLocation;

    public PluginData(PluginClass pluginClass, ArtifactRange usableBy, Location artifactLocation) {
      this.pluginClass = pluginClass;
      this.parentVersionLower = usableBy.getLower().getVersion();
      this.parentVersionUpper = usableBy.getUpper().getVersion();
      this.artifactLocation = artifactLocation;
    }
  }
}
