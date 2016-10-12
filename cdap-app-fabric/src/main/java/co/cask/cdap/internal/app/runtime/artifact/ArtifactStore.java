/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.common.ArtifactAlreadyExistsException;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.internal.app.deploy.pipeline.NamespacedImpersonator;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ApplicationClass;
import co.cask.cdap.proto.artifact.ArtifactClasses;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;

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
 * rowkey r:{namespace}:{artifact-name}, column {artifact-version}, and ArtifactData as the value
 *
 * The second adds metadata about any Application Class contained in the artifact, with
 * rowkey a:{namespace}:{classname}, column {artifact-name}:{artifact-version}, and AppClass as the value
 *
 * The third adds metadata about any Plugin contained in the artifact, with
 * rowkey p:{parent-namespace}:{parent-name}:{plugin-type}:{plugin-name},
 * column {artifact-namespace}:{artifact-name}:{artifact-version},
 * and PluginData as the value
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
  private static final String APPCLASS_PREFIX = "a";
  private static final DatasetId META_ID = NamespaceId.SYSTEM.dataset("artifact.meta");
  private static final DatasetProperties META_PROPERTIES =
    DatasetProperties.builder().add(Table.PROPERTY_CONFLICT_LEVEL, ConflictDetection.COLUMN.name()).build();

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(ArtifactRange.class, new ArtifactRangeCodec())
    .create();

  private final LocationFactory locationFactory;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final Impersonator impersonator;

  @Inject
  ArtifactStore(DatasetFramework datasetFramework,
                NamespacedLocationFactory namespacedLocationFactory,
                LocationFactory locationFactory,
                TransactionSystemClient txClient,
                Impersonator impersonator) {
    this.locationFactory = locationFactory;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                                   txClient, META_ID.getParent(),
                                                                   Collections.<String, String>emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.impersonator = impersonator;
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by artifact store.
   *
   * @param framework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(Table.class.getName(), META_ID.toId(), META_PROPERTIES);
  }

  private Table getMetaTable(DatasetContext context) throws IOException, DatasetManagementException {
    return DatasetsUtil.getOrCreateDataset(context, datasetFramework, META_ID, Table.class.getName(), META_PROPERTIES);
  }

  /**
   * Get information about all artifacts in the given namespace. If there are no artifacts in the namespace,
   * this will return an empty list. Note that existence of the namespace is not checked.
   *
   * @param namespace the namespace to get artifact information about
   * @return unmodifiable list of artifact info about every artifact in the given namespace
   * @throws IOException if there was an exception reading the artifact information from the metastore
   */
  public List<ArtifactDetail> getArtifacts(final NamespaceId namespace) throws IOException {
    try {
      return Transactions.execute(transactional, new TxCallable<List<ArtifactDetail>>() {
        @Override
        public List<ArtifactDetail> call(DatasetContext context) throws Exception {
          List<ArtifactDetail> artifacts = Lists.newArrayList();
          try (Scanner scanner = getMetaTable(context).scan(scanArtifacts(namespace))) {
            Row row;
            while ((row = scanner.next()) != null) {
              addArtifactsToList(artifacts, row);
            }
          }
          return Collections.unmodifiableList(artifacts);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  /**
   * Get all artifacts that match artifacts in the given ranges.
   *
   * @param range the range to match artifacts in
   * @return an unmodifiable list of all artifacts that match the given ranges. If none exist, an empty list
   *         is returned
   */
  public List<ArtifactDetail> getArtifacts(final ArtifactRange range) {
    try {
      return Transactions.execute(transactional, new TxCallable<List<ArtifactDetail>>() {
        @Override
        public List<ArtifactDetail> call(DatasetContext context) throws Exception {
          List<ArtifactDetail> artifacts = Lists.newArrayList();
          ArtifactKey artifactKey = new ArtifactKey(range.getNamespace().toEntityId(), range.getName());

          Row row = getMetaTable(context).get(artifactKey.getRowKey());
          for (Map.Entry<byte[], byte[]> columnEntry : row.getColumns().entrySet()) {
            String version = Bytes.toString(columnEntry.getKey());
            if (range.versionIsInRange(new ArtifactVersion(version))) {
              ArtifactData data = GSON.fromJson(Bytes.toString(columnEntry.getValue()), ArtifactData.class);
              Id.Artifact artifactId = Id.Artifact.from(artifactKey.namespace.toId(), artifactKey.name, version);
              artifacts.add(new ArtifactDetail(new ArtifactDescriptor(
                artifactId.toArtifactId(), locationFactory.create(data.locationURI)), data.meta));
            }
          }
          return Collections.unmodifiableList(artifacts);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  /**
   * Get information about all versions of the given artifact.
   *
   * @param namespace the namespace to get artifacts from
   * @param artifactName the name of the artifact to get
   * @return unmodifiable list of information about all versions of the given artifact
   * @throws ArtifactNotFoundException if no version of the given artifact exists
   * @throws IOException if there was an exception reading the artifact information from the metastore
   */
  public List<ArtifactDetail> getArtifacts(final NamespaceId namespace,
                                           final String artifactName) throws ArtifactNotFoundException, IOException {
    try {
      return Transactions.execute(transactional, new TxCallable<List<ArtifactDetail>>() {
        @Override
        public List<ArtifactDetail> call(DatasetContext context) throws Exception {
          List<ArtifactDetail> artifacts = Lists.newArrayList();
          ArtifactKey artifactKey = new ArtifactKey(namespace, artifactName);
          Row row = getMetaTable(context).get(artifactKey.getRowKey());
          if (!row.isEmpty()) {
            addArtifactsToList(artifacts, row);
          }
          if (artifacts.isEmpty()) {
            throw new ArtifactNotFoundException(namespace.toId(), artifactName);
          }

          return Collections.unmodifiableList(artifacts);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, ArtifactNotFoundException.class, IOException.class);
    }
  }

  /**
   * Get information about the given artifact.
   *
   * @param artifactId the artifact to get
   * @return information about the artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception reading the artifact information from the metastore
   */
  public ArtifactDetail getArtifact(final Id.Artifact artifactId) throws ArtifactNotFoundException, IOException {
    try {
      final ArtifactData artifactData = Transactions.execute(transactional, new TxCallable<ArtifactData>() {
        @Override
        public ArtifactData call(DatasetContext context) throws Exception {
          ArtifactCell artifactCell = new ArtifactCell(artifactId);
          byte[] value = getMetaTable(context).get(artifactCell.rowkey, artifactCell.column);
          if (value == null) {
            throw new ArtifactNotFoundException(artifactId);
          }
          return GSON.fromJson(Bytes.toString(value), ArtifactData.class);
        }
      });

      Location artifactLocation = impersonator.doAs(artifactId.getNamespace().toEntityId(), new Callable<Location>() {
        @Override
        public Location call() throws Exception {
          return locationFactory.create(artifactData.locationURI);
        }
      });
      return new ArtifactDetail(new ArtifactDescriptor(artifactId.toArtifactId(), artifactLocation), artifactData.meta);
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class, ArtifactNotFoundException.class);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Get all application classes that belong to the specified namespace.
   * Results are returned as a sorted map from artifact to application classes in that artifact.
   * Map entries are sorted by the artifact.
   *
   * @param namespace the namespace from which to get application classes
   * @return an unmodifiable map of artifact to a list of all application classes in that artifact.
   *         The map will never be null. If there are no application classes, an empty map will be returned.
   */
  public SortedMap<ArtifactDescriptor, List<ApplicationClass>> getApplicationClasses(final NamespaceId namespace) {
    try {
      return Transactions.execute(
        transactional, new TxCallable<SortedMap<ArtifactDescriptor, List<ApplicationClass>>>() {
          @Override
          public SortedMap<ArtifactDescriptor, List<ApplicationClass>> call(DatasetContext context) throws Exception {
            SortedMap<ArtifactDescriptor, List<ApplicationClass>> result = Maps.newTreeMap();
            try (Scanner scanner = getMetaTable(context).scan(scanAppClasses(namespace))) {
              Row row;
              while ((row = scanner.next()) != null) {
                // columns are {artifact-name}:{artifact-version}. vals are serialized AppData
                for (Map.Entry<byte[], byte[]> column : row.getColumns().entrySet()) {
                  ArtifactColumn artifactColumn = ArtifactColumn.parse(column.getKey());
                  AppData appData = GSON.fromJson(Bytes.toString(column.getValue()), AppData.class);

                  ArtifactDescriptor artifactDescriptor = new ArtifactDescriptor(
                    artifactColumn.artifactId.toArtifactId(), locationFactory.create(appData.artifactLocationURI));
                  List<ApplicationClass> existingAppClasses = result.get(artifactDescriptor);
                  if (existingAppClasses == null) {
                    existingAppClasses = new ArrayList<>();
                    result.put(artifactDescriptor, existingAppClasses);
                  }
                  existingAppClasses.add(appData.appClass);
                }
              }
            }
            return Collections.unmodifiableSortedMap(result);
          }
        });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  /**
   * Get all application classes that belong to the specified namespace of the specified classname.
   * Results are returned as a sorted map from artifact to application classes in that artifact.
   * Map entries are sorted by the artifact.
   *
   * @param namespace the namespace from which to get application classes
   * @param className the classname of application classes to get
   * @return an unmodifiable map of artifact the application classes in that artifact.
   *         The map will never be null. If there are no application classes, an empty map will be returned.
   */
  public SortedMap<ArtifactDescriptor, ApplicationClass> getApplicationClasses(final NamespaceId namespace,
                                                                               final String className) {
    try {
      return Transactions.execute(transactional, new TxCallable<SortedMap<ArtifactDescriptor, ApplicationClass>>() {
        @Override
        public SortedMap<ArtifactDescriptor, ApplicationClass> call(DatasetContext context) throws Exception {
          SortedMap<ArtifactDescriptor, ApplicationClass> result = Maps.newTreeMap();
          Row row = getMetaTable(context).get(new AppClassKey(namespace, className).getRowKey());
          if (!row.isEmpty()) {
            // columns are {artifact-name}:{artifact-version}. vals are serialized AppData
            for (Map.Entry<byte[], byte[]> column : row.getColumns().entrySet()) {
              ArtifactColumn artifactColumn = ArtifactColumn.parse(column.getKey());
              AppData appData = GSON.fromJson(Bytes.toString(column.getValue()), AppData.class);

              ArtifactDescriptor artifactDescriptor = new ArtifactDescriptor(
                artifactColumn.artifactId.toArtifactId(), locationFactory.create(appData.artifactLocationURI));
              result.put(artifactDescriptor, appData.appClass);
            }
          }
          return Collections.unmodifiableSortedMap(result);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  /**
   * Get all plugin classes that extend the given parent artifact.
   * Results are returned as a sorted map from plugin artifact to plugins in that artifact.
   * Map entries are sorted by the artifact id of the plugin.
   *
   * @param parentArtifactId the id of the artifact to find plugins for
   * @return an unmodifiable map of plugin artifact to plugin classes for all plugin classes accessible by the given
   *         artifact. The map will never be null. If there are no plugin classes, an empty map will be returned.
   * @throws ArtifactNotFoundException if the artifact to find plugins for does not exist
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPluginClasses(final NamespaceId namespace,
                                                                          final Id.Artifact parentArtifactId)
    throws ArtifactNotFoundException, IOException {

    try {
      return Transactions.execute(transactional, new TxCallable<SortedMap<ArtifactDescriptor, Set<PluginClass>>>() {
        @Override
        public SortedMap<ArtifactDescriptor, Set<PluginClass>> call(DatasetContext context) throws Exception {
          Table metaTable = getMetaTable(context);

          SortedMap<ArtifactDescriptor, Set<PluginClass>> plugins = getPluginsInArtifact(metaTable, parentArtifactId);
          if (plugins == null) {
            throw new ArtifactNotFoundException(parentArtifactId);
          }

          // should be able to scan by column prefix as well... instead, we have to filter out by namespace
          try (Scanner scanner = metaTable.scan(scanPlugins(parentArtifactId))) {
            Row row;
            while ((row = scanner.next()) != null) {
              addPluginsToMap(namespace, parentArtifactId, plugins, row);
            }
          }
          return Collections.unmodifiableSortedMap(plugins);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, ArtifactNotFoundException.class, IOException.class);
    }
  }

  /**
   * Get all plugin classes of the given type that extend the given parent artifact.
   * Results are returned as a map from plugin artifact to plugins in that artifact.
   *
   * @param parentArtifactId the id of the artifact to find plugins for
   * @param type the type of plugin to look for
   * @return an unmodifiable map of plugin artifact to plugin classes for all plugin classes accessible by the
   *         given artifact. The map will never be null. If there are no plugin classes, an empty map will be returned.
   * @throws ArtifactNotFoundException if the artifact to find plugins for does not exist
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPluginClasses(final NamespaceId namespace,
                                                                          final Id.Artifact parentArtifactId,
                                                                          final String type)
    throws IOException, ArtifactNotFoundException {

    try {
      return Transactions.execute(transactional, new TxCallable<SortedMap<ArtifactDescriptor, Set<PluginClass>>>() {
        @Override
        public SortedMap<ArtifactDescriptor, Set<PluginClass>> call(DatasetContext context) throws Exception {
          Table metaTable = getMetaTable(context);
          SortedMap<ArtifactDescriptor, Set<PluginClass>> plugins = getPluginsInArtifact(
            metaTable, parentArtifactId, new Predicate<PluginClass>() {
              @Override
              public boolean apply(PluginClass input) {
                return type.equals(input.getType());
              }
            });

          if (plugins == null) {
            throw new ArtifactNotFoundException(parentArtifactId);
          }

          try (Scanner scanner = metaTable.scan(scanPlugins(parentArtifactId, type))) {
            Row row;
            while ((row = scanner.next()) != null) {
              addPluginsToMap(namespace, parentArtifactId, plugins, row);
            }
          }

          return Collections.unmodifiableSortedMap(plugins);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, ArtifactNotFoundException.class, IOException.class);
    }
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
  public SortedMap<ArtifactDescriptor, PluginClass> getPluginClasses(final NamespaceId namespace,
                                                                     final Id.Artifact parentArtifactId,
                                                                     final String type, final String name)
    throws IOException, ArtifactNotFoundException, PluginNotExistsException {

    try {
      SortedMap<ArtifactDescriptor, PluginClass> result =
        Transactions.execute(transactional, new TxCallable<SortedMap<ArtifactDescriptor, PluginClass>>() {
          @Override
          public SortedMap<ArtifactDescriptor, PluginClass> call(DatasetContext context) throws Exception {
            Table metaTable = getMetaTable(context);

            // check parent exists
            ArtifactCell parentCell = new ArtifactCell(parentArtifactId);
            byte[] parentDataBytes = metaTable.get(parentCell.rowkey, parentCell.column);
            if (parentDataBytes == null) {
              throw new ArtifactNotFoundException(parentArtifactId);
            }

            SortedMap<ArtifactDescriptor, PluginClass> plugins = new TreeMap<>();

            // check if any plugins of that type and name exist in the parent artifact already
            ArtifactData parentData = GSON.fromJson(Bytes.toString(parentDataBytes), ArtifactData.class);
            Set<PluginClass> parentPlugins = parentData.meta.getClasses().getPlugins();
            for (PluginClass pluginClass : parentPlugins) {
              if (pluginClass.getName().equals(name) && pluginClass.getType().equals(type)) {
                ArtifactDescriptor parentDescriptor = new ArtifactDescriptor(
                  parentArtifactId.toArtifactId(), locationFactory.create(parentData.locationURI));
                plugins.put(parentDescriptor, pluginClass);
                break;
              }
            }

            PluginKey pluginKey = new PluginKey(parentArtifactId.getNamespace(),
                                                parentArtifactId.getName(), type, name);
            Row row = metaTable.get(pluginKey.getRowKey());
            if (!row.isEmpty()) {
              // column is the artifact name and version, value is the serialized PluginClass
              for (Map.Entry<byte[], byte[]> column : row.getColumns().entrySet()) {
                ImmutablePair<ArtifactDescriptor, PluginClass> pluginEntry =
                  getPluginEntry(namespace, parentArtifactId, column);

                if (pluginEntry != null) {
                  plugins.put(pluginEntry.getFirst(), pluginEntry.getSecond());
                }
              }
            }

            return Collections.unmodifiableSortedMap(plugins);
          }
        });

      if (result.isEmpty()) {
        throw new PluginNotExistsException(parentArtifactId.getNamespace(), type, name);
      }
      return result;
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class, ArtifactNotFoundException.class);
    }
  }

  /**
   * Update artifact properties using an update function. Functions will receive an immutable map.
   *
   * @param artifactId the id of the artifact to add
   * @param updateFunction the function used to update existing properties
   * @throws ArtifactNotFoundException if the artifact does not exist
   * @throws IOException if there was an exception writing the properties to the metastore
   */
  public void updateArtifactProperties(final Id.Artifact artifactId,
                                       final Function<Map<String, String>, Map<String, String>> updateFunction)
    throws ArtifactNotFoundException, IOException {

    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          ArtifactCell artifactCell = new ArtifactCell(artifactId);
          Table metaTable = getMetaTable(context);
          byte[] existingMetaBytes = metaTable.get(artifactCell.rowkey, artifactCell.column);
          if (existingMetaBytes == null) {
            throw new ArtifactNotFoundException(artifactId);
          }

          ArtifactData old = GSON.fromJson(Bytes.toString(existingMetaBytes), ArtifactData.class);
          ArtifactMeta updatedMeta = new ArtifactMeta(old.meta.getClasses(), old.meta.getUsableBy(),
                                                      updateFunction.apply(old.meta.getProperties()));
          ArtifactData updatedData = new ArtifactData(locationFactory.create(old.locationURI), updatedMeta);
          // write artifact metadata
          metaTable.put(artifactCell.rowkey, artifactCell.column, Bytes.toBytes(GSON.toJson(updatedData)));
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, ArtifactNotFoundException.class, IOException.class);
    }
  }

  /**
   * Write the artifact and its metadata to the store. Once added, artifacts cannot be changed unless they are
   * snapshot versions.
   *
   * @param artifactId the id of the artifact to add
   * @param artifactMeta the metadata for the artifact
   * @param artifactContentSupplier the supplier for the input stream of the contents of the artifact
   * @return detail about the newly added artifact
   * @throws WriteConflictException if the artifact is already currently being written
   * @throws ArtifactAlreadyExistsException if a non-snapshot version of the artifact already exists
   * @throws IOException if there was an exception persisting the artifact contents to the filesystem,
   *                     of persisting the artifact metadata to the metastore
   */
  public ArtifactDetail write(final Id.Artifact artifactId,
                              final ArtifactMeta artifactMeta,
                              final InputSupplier<? extends InputStream> artifactContentSupplier,
                              NamespacedImpersonator namespacedImpersonator)
    throws WriteConflictException, ArtifactAlreadyExistsException, IOException {

    // if we're not a snapshot version, check that the artifact doesn't exist already.
    final ArtifactCell artifactCell = new ArtifactCell(artifactId);
    if (!artifactId.getVersion().isSnapshot()) {
      try {
        transactional.execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            if (getMetaTable(context).get(artifactCell.rowkey, artifactCell.column) != null) {
              throw new ArtifactAlreadyExistsException(artifactId);
            }
          }
        });
      } catch (TransactionFailureException e) {
        throw Transactions.propagate(e, ArtifactAlreadyExistsException.class, IOException.class);
      }
    }

    final Location destination;
    try {
      destination = copyFileToDestination(artifactId, artifactContentSupplier, namespacedImpersonator);
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }

    // now try and write the metadata for the artifact
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          // we have to check that the metadata doesn't exist again since somebody else may have written
          // the artifact while we were copying the artifact to the filesystem.
          Table metaTable = getMetaTable(context);
          byte[] existingMetaBytes = metaTable.get(artifactCell.rowkey, artifactCell.column);
          boolean isSnapshot = artifactId.getVersion().isSnapshot();
          if (existingMetaBytes != null && !isSnapshot) {
            // non-snapshot artifacts are immutable. If there is existing metadata, stop here.
            throw new ArtifactAlreadyExistsException(artifactId);
          }

          ArtifactData data = new ArtifactData(destination, artifactMeta);
          // cleanup existing metadata if it exists and this is a snapshot
          // if we are overwriting a previous snapshot, need to clean up the old snapshot data
          // this means cleaning up the old jar, and deleting plugin and app rows.
          if (existingMetaBytes != null) {
            deleteMeta(metaTable, artifactId, existingMetaBytes);
          }
          // write artifact metadata
          writeMeta(metaTable, artifactId, data);
        }
      });

      return new ArtifactDetail(new ArtifactDescriptor(artifactId.toArtifactId(), destination), artifactMeta);
    } catch (TransactionConflictException e) {
      destination.delete();
      throw new WriteConflictException(artifactId);
    } catch (TransactionFailureException e) {
      destination.delete();
      throw Transactions.propagate(e, ArtifactAlreadyExistsException.class, IOException.class);
    }
  }

  private Location copyFileToDestination(final Id.Artifact artifactId,
                                         final InputSupplier<? extends InputStream> artifactContentSupplier,
                                         NamespacedImpersonator namespacedImpersonator) throws Exception {
    return namespacedImpersonator.impersonate(new Callable<Location>() {
      @Override
      public Location call() throws IOException {
        return copyFile(artifactId, artifactContentSupplier);
      }
    });
  }

  private Location copyFile(Id.Artifact artifactId,
                            InputSupplier<? extends InputStream> artifactContentSupplier) throws IOException {
    Location fileDirectory = namespacedLocationFactory.get(artifactId.getNamespace(),
                                                           ARTIFACTS_PATH).append(artifactId.getName());
    Location destination = fileDirectory.append(artifactId.getVersion().getVersion()).getTempFile(".jar");
    Locations.mkdirsIfNotExists(fileDirectory);
    // write the file contents
    try (InputStream artifactContents = artifactContentSupplier.getInput();
         OutputStream destinationStream = destination.getOutputStream()) {
      ByteStreams.copy(artifactContents, destinationStream);
    }
    return destination;
  }

  /**
   * Delete the specified artifact. Programs that use the artifact will no longer be able to start.
   *
   * @param artifactId the id of the artifact to delete
   * @throws IOException if there was an IO error deleting the metadata or the actual artifact
   */
  public void delete(final Id.Artifact artifactId) throws IOException {

    // delete everything in a transaction
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          // first look up details to get plugins and apps in the artifact
          ArtifactCell artifactCell = new ArtifactCell(artifactId);
          Table metaTable = getMetaTable(context);
          byte[] detailBytes = metaTable.get(artifactCell.rowkey, artifactCell.column);
          if (detailBytes == null) {
            // ok there is nothing to delete, we're done
            return;
          }
          deleteMeta(metaTable, artifactId, detailBytes);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  /**
   * Clear all data in the given namespace. Used only in unit tests.
   *
   * @param namespace the namespace to delete data in
   * @throws IOException if there was some problem deleting the data
   */
  @VisibleForTesting
  void clear(final NamespaceId namespace) throws IOException {
    final Id.Namespace namespaceId = namespace.toId();
    namespacedLocationFactory.get(namespaceId, ARTIFACTS_PATH).delete(true);

    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          // delete all rows about artifacts in the namespace
          Table metaTable = getMetaTable(context);
          Row row;
          try (Scanner scanner = metaTable.scan(scanArtifacts(namespace))) {
            while ((row = scanner.next()) != null) {
              metaTable.delete(row.getRow());
            }
          }

          // delete all rows about artifacts in the namespace and the plugins they have access to
          Scan pluginsScan = new Scan(
            Bytes.toBytes(String.format("%s:%s:", PLUGIN_PREFIX, namespace.getNamespace())),
            Bytes.toBytes(String.format("%s:%s;", PLUGIN_PREFIX, namespace.getNamespace()))
          );
          try (Scanner scanner = metaTable.scan(pluginsScan)) {
            while ((row = scanner.next()) != null) {
              metaTable.delete(row.getRow());
            }
          }

          // delete app classes in this namespace
          try (Scanner scanner = metaTable.scan(scanAppClasses(namespace))) {
            while ((row = scanner.next()) != null) {
              metaTable.delete(row.getRow());
            }
          }

          // delete plugins in this namespace from system artifacts
          // for example, if there was an artifact in this namespace that extends a system artifact
          Scan systemPluginsScan = new Scan(
            Bytes.toBytes(String.format("%s:%s:", PLUGIN_PREFIX, Id.Namespace.SYSTEM.getId())),
            Bytes.toBytes(String.format("%s:%s;", PLUGIN_PREFIX, Id.Namespace.SYSTEM.getId()))
          );
          try (Scanner scanner = metaTable.scan(systemPluginsScan)) {
            while ((row = scanner.next()) != null) {
              for (Map.Entry<byte[], byte[]> columnVal : row.getColumns().entrySet()) {
                // the column is the id of the artifact the plugin is from
                ArtifactColumn column = ArtifactColumn.parse(columnVal.getKey());
                // if the plugin artifact is in the namespace we're deleting, delete this column.
                if (column.artifactId.getNamespace().equals(namespaceId)) {
                  metaTable.delete(row.getRow(), column.getColumn());
                }
              }
            }
          }
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  // write a new artifact snapshot and clean up the old snapshot data
  private void writeMeta(Table table, Id.Artifact artifactId, ArtifactData data) throws IOException {
    ArtifactCell artifactCell = new ArtifactCell(artifactId);
    table.put(artifactCell.rowkey, artifactCell.column, Bytes.toBytes(GSON.toJson(data)));

    // column for plugin meta and app meta. {artifact-name}:{artifact-version}
    // does not need to contain namespace because namespace is in the rowkey
    byte[] artifactColumn = new ArtifactColumn(artifactId).getColumn();

    ArtifactClasses classes = data.meta.getClasses();
    Location artifactLocation = locationFactory.create(data.locationURI);
    // write pluginClass metadata
    for (PluginClass pluginClass : classes.getPlugins()) {
      // write metadata for each artifact this plugin extends
      for (ArtifactRange artifactRange : data.meta.getUsableBy()) {
        // p:{namespace}:{type}:{name}
        PluginKey pluginKey = new PluginKey(
          artifactRange.getNamespace(), artifactRange.getName(), pluginClass.getType(), pluginClass.getName());

        byte[] pluginDataBytes = Bytes.toBytes(
          GSON.toJson(new PluginData(pluginClass, artifactRange, artifactLocation)));
        table.put(pluginKey.getRowKey(), artifactColumn, pluginDataBytes);
      }
    }

    // write appClass metadata
    for (ApplicationClass appClass : classes.getApps()) {
      // a:{namespace}:{classname}
      AppClassKey appClassKey = new AppClassKey(artifactId.getNamespace().toEntityId(), appClass.getClassName());
      byte[] appDataBytes = Bytes.toBytes(GSON.toJson(new AppData(appClass, artifactLocation)));
      table.put(appClassKey.getRowKey(), artifactColumn, appDataBytes);
    }
  }

  private void deleteMeta(Table table, Id.Artifact artifactId, byte[] oldData) throws IOException {
    // delete old artifact data
    ArtifactCell artifactCell = new ArtifactCell(artifactId);
    table.delete(artifactCell.rowkey, artifactCell.column);

    // delete old plugins
    final ArtifactData oldMeta = GSON.fromJson(Bytes.toString(oldData), ArtifactData.class);
    byte[] artifactColumn = new ArtifactColumn(artifactId).getColumn();

    for (PluginClass pluginClass : oldMeta.meta.getClasses().getPlugins()) {
      // delete metadata for each artifact this plugin extends
      for (ArtifactRange artifactRange : oldMeta.meta.getUsableBy()) {
        // p:{namespace}:{type}:{name}
        PluginKey pluginKey = new PluginKey(
          artifactRange.getNamespace(), artifactRange.getName(), pluginClass.getType(), pluginClass.getName());
        table.delete(pluginKey.getRowKey(), artifactColumn);
      }
    }

    // delete old appclass metadata
    for (ApplicationClass appClass : oldMeta.meta.getClasses().getApps()) {
      AppClassKey appClassKey = new AppClassKey(artifactId.getNamespace().toEntityId(), appClass.getClassName());
      table.delete(appClassKey.getRowKey(), artifactColumn);
    }

    // delete the old jar file

    try {
      new NamespacedImpersonator(artifactId.getNamespace().toEntityId(),
                                 impersonator).impersonate(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          locationFactory.create(oldMeta.locationURI).delete();
          return null;
        }
      });
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      // this should not happen
      throw Throwables.propagate(e);
    }
  }


  private SortedMap<ArtifactDescriptor, Set<PluginClass>> getPluginsInArtifact(Table table, Id.Artifact artifactId) {
    return getPluginsInArtifact(table, artifactId, Predicates.<PluginClass>alwaysTrue());
  }

  private SortedMap<ArtifactDescriptor, Set<PluginClass>> getPluginsInArtifact(Table table, Id.Artifact artifactId,
                                                                               Predicate<PluginClass> filter) {
    SortedMap<ArtifactDescriptor, Set<PluginClass>> result = new TreeMap<>();

    // Make sure the artifact exists
    ArtifactCell parentCell = new ArtifactCell(artifactId);
    byte[] parentDataBytes = table.get(parentCell.rowkey, parentCell.column);
    if (parentDataBytes == null) {
      return null;
    }

    // include any plugin classes that are inside the artifact itself
    ArtifactData parentData = GSON.fromJson(Bytes.toString(parentDataBytes), ArtifactData.class);
    Set<PluginClass> parentPlugins = parentData.meta.getClasses().getPlugins();

    Set<PluginClass> filteredPlugins = Sets.newLinkedHashSet(Iterables.filter(parentPlugins, filter));

    if (!filteredPlugins.isEmpty()) {
      Location parentLocation = locationFactory.create(parentData.locationURI);
      ArtifactDescriptor descriptor = new ArtifactDescriptor(artifactId.toArtifactId(), parentLocation);
      result.put(descriptor, filteredPlugins);
    }
    return result;
  }

  private void addArtifactsToList(List<ArtifactDetail> artifactDetails, Row row) throws IOException {
    ArtifactKey artifactKey = ArtifactKey.parse(row.getRow());

    for (Map.Entry<byte[], byte[]> columnVal : row.getColumns().entrySet()) {
      String version = Bytes.toString(columnVal.getKey());
      ArtifactData data = GSON.fromJson(Bytes.toString(columnVal.getValue()), ArtifactData.class);
      Id.Artifact artifactId = Id.Artifact.from(artifactKey.namespace.toId(), artifactKey.name, version);
      artifactDetails.add(new ArtifactDetail(
        new ArtifactDescriptor(artifactId.toArtifactId(), locationFactory.create(data.locationURI)),
        data.meta));
    }
  }

  // this method examines all plugins in the given row and checks if they extend the given parent artifact
  // and are from an artifact in the given namespace.
  // if so, information about the plugin artifact and the plugin details are added to the given map.
  private void addPluginsToMap(NamespaceId namespace, Id.Artifact parentArtifactId,
                               SortedMap<ArtifactDescriptor, Set<PluginClass>> map,
                               Row row) throws IOException {
    // column is the artifact namespace, name, and version. value is the serialized PluginData
    for (Map.Entry<byte[], byte[]> column : row.getColumns().entrySet()) {
      ImmutablePair<ArtifactDescriptor, PluginClass> pluginEntry = getPluginEntry(namespace, parentArtifactId, column);
      if (pluginEntry != null) {
        ArtifactDescriptor artifactDescriptor = pluginEntry.getFirst();
        if (!map.containsKey(artifactDescriptor)) {
          map.put(artifactDescriptor, Sets.<PluginClass>newHashSet());
        }
        map.get(artifactDescriptor).add(pluginEntry.getSecond());
      }
    }
  }

  /**
   * Decode the PluginClass from the table column if it is from an artifact in the given namespace and
   * extends the given parent artifact. If the plugin's artifact is not in the given namespace, or it does not
   * extend the given parent artifact, return null.
   */
  private ImmutablePair<ArtifactDescriptor, PluginClass> getPluginEntry(NamespaceId namespace,
                                                                        Id.Artifact parentArtifactId,
                                                                        Map.Entry<byte[], byte[]> column) {
    // column is the artifact namespace, name, and version. value is the serialized PluginData
    ArtifactColumn artifactColumn = ArtifactColumn.parse(column.getKey());
    Id.Namespace artifactNamespace = artifactColumn.artifactId.getNamespace();
    // filter out plugins whose artifacts are not in the system namespace and not in this namespace
    if (!Id.Namespace.SYSTEM.equals(artifactNamespace) && !artifactNamespace.equals(namespace.toId())) {
      return null;
    }
    PluginData pluginData = GSON.fromJson(Bytes.toString(column.getValue()), PluginData.class);

    // filter out plugins that don't extend this version of the parent artifact
    if (pluginData.usableBy.versionIsInRange(parentArtifactId.getVersion())) {
      ArtifactDescriptor artifactDescriptor = new ArtifactDescriptor(
        artifactColumn.artifactId.toArtifactId(), locationFactory.create(pluginData.artifactLocationURI));

      return ImmutablePair.of(artifactDescriptor, pluginData.pluginClass);
    }
    return null;
  }

  private Scan scanArtifacts(NamespaceId namespace) {
    return new Scan(
      Bytes.toBytes(String.format("%s:%s:", ARTIFACT_PREFIX, namespace.getNamespace())),
      Bytes.toBytes(String.format("%s:%s;", ARTIFACT_PREFIX, namespace.getNamespace())));
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
                                  PLUGIN_PREFIX, parentArtifactId.getNamespace().getId(),
                                  parentArtifactId.getName(), type)),
      Bytes.toBytes(String.format("%s:%s:%s:%s;",
                                  PLUGIN_PREFIX, parentArtifactId.getNamespace().getId(),
                                  parentArtifactId.getName(), type)));
  }

  private Scan scanAppClasses(NamespaceId namespace) {
    return new Scan(
      Bytes.toBytes(String.format("%s:%s:", APPCLASS_PREFIX, namespace.getNamespace())),
      Bytes.toBytes(String.format("%s:%s;", APPCLASS_PREFIX, namespace.getNamespace())));
  }

  private static class AppClassKey {
    private final NamespaceId namespace;
    private final String className;

    AppClassKey(NamespaceId namespace, String className) {
      this.namespace = namespace;
      this.className = className;
    }

    private byte[] getRowKey() {
      return Bytes.toBytes(Joiner.on(':').join(APPCLASS_PREFIX, namespace.getNamespace(), className));
    }
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
                                         artifactId.getNamespace().getId(), artifactId.getName(),
                                         artifactId.getVersion().getVersion()));
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
    private final NamespaceId namespace;
    private final String name;

    private ArtifactKey(NamespaceId namespace, String name) {
      this.namespace = namespace;
      this.name = name;
    }

    private byte[] getRowKey() {
      return Bytes.toBytes(Joiner.on(':').join(ARTIFACT_PREFIX, namespace.getNamespace(), name));
    }

    private static ArtifactKey parse(byte[] rowkey) {
      String key = Bytes.toString(rowkey);
      Iterator<String> parts = Splitter.on(':').limit(4).split(key).iterator();
      // first part is the artifact prefix
      parts.next();
      // next is namespace, then name
      return new ArtifactKey(Ids.namespace(parts.next()), parts.next());
    }
  }

  private static class ArtifactCell {
    private final byte[] rowkey;
    private final byte[] column;

    private ArtifactCell(Id.Artifact artifactId) {
      rowkey = new ArtifactKey(artifactId.getNamespace().toEntityId(), artifactId.getName()).getRowKey();
      column = Bytes.toBytes(artifactId.getVersion().getVersion());
    }
  }

  // Data that will be stored for an artifact. Same as ArtifactDetail, expected without the id since that is redundant.
  private static class ArtifactData {
    private final URI locationURI;
    private final ArtifactMeta meta;

    ArtifactData(Location location, ArtifactMeta meta) {
      this.locationURI = location.toURI();
      this.meta = meta;
    }
  }

  // Data that will be stored for a plugin.
  private static class PluginData {
    private final PluginClass pluginClass;
    private final ArtifactRange usableBy;
    private final URI artifactLocationURI;

    PluginData(PluginClass pluginClass, ArtifactRange usableBy, Location artifactLocation) {
      this.pluginClass = pluginClass;
      this.usableBy = usableBy;
      this.artifactLocationURI = artifactLocation.toURI();
    }
  }

  // Data that will be stored for an application class.
  private static class AppData {
    private final ApplicationClass appClass;
    private final URI artifactLocationURI;

    AppData(ApplicationClass appClass, Location artifactLocation) {
      this.appClass = appClass;
      this.artifactLocationURI = artifactLocation.toURI();
    }
  }
}
