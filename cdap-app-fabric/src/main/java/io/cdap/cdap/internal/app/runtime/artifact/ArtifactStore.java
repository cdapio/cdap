/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

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
 * This class will store 4 kinds of information: artifact metadata, appClass metadata, plugin metadata, universal
 * plugin metadata, so it will use 4 tables to store all the information.
 *
 * The first table is artifact_data table, it has 4 fields: artifact namespace, artifact name, artifact version, and
 * artifact data with the first three fields as the primary keys.
 *
 * The second table is app_data table, it has 6 fields: app namespace, app class name, artifact namespace,
 * artifact name, artifact version, and app data with first 5 fields as the primary keys.
 *
 * The third table is plugin_data table, it has 8 fields: parent artifact namespace, parent artifact name, plugin type,
 * plugin name, artifact namespace, artifact name, artifact version, and plugin data with first 7 fields as the
 * primary keys.
 *
 * The last table is the universal_plugin_data table, it has 7 fields: plugin namespace, plugin type, plugin name,
 * artifact namespace, artifact name, artifact version and plugin data with first 6 fields as the primary keys.
 *
 * For example, suppose we add a system artifact etlbatch-3.1.0, which contains an ETLBatch application class.
 * The meta table will look like:
 *
 * table                             primary keys                                                  value
 * app_data                          system, ETLBatch, system, etlbatch, 3.1.0                    {@link AppData}
 * artifact_data                     system, etlbatch, 3.1.0                                      {@link ArtifactData}
 *
 * After that, a system artifact etlbatch-lib-3.1.0 is added, which extends etlbatch and contains
 * stream sink and table sink plugins. The meta table will look like:
 *
 * table                             primary keys                                                  value
 * app_data                          system, ETLBatch, system, etlbatch, 3.1.0                    {@link AppData}
 * artifact_data                     system, etlbatch, 3.1.0                                      {@link ArtifactData}
 * artifact_data                     system, etlbatch-lib, 3.1.0                                  {@link ArtifactData}
 * plugin_data                       system, etlbatch, sink, stream, system, etlbatch-lib, 3.1.0  {@link PluginData}
 * plugin_data                       system, etlbatch, sink, table, system, etlbatch-lib, 3.1.0   {@link PluginData}
 *
 * Finally a user adds artifact custom-sources-1.0.0 to the default namespace,
 * which extends etlbatch and contains a db source plugin. The meta table will look like:
 *
 * table                             primary keys                                                  value
 * app_data                          system, ETLBatch, system, etlbatch, 3.1.0                    {@link AppData}
 * artifact_data                     default, custom-sources, 1.0.0                               {@link ArtifactData}
 * artifact_data                     system, etlbatch, 3.1.0                                      {@link ArtifactData}
 * artifact_data                     system, etlbatch-lib, 3.1.0                                  {@link ArtifactData}
 * plugin_data                       system, etlbatch, sink, stream, system, etlbatch-lib, 3.1.0  {@link PluginData}
 * plugin_data                       system, etlbatch, sink, table, system, etlbatch-lib, 3.1.0   {@link PluginData}
 * plugin_data                       system, etlbatch, source, db, default, custom-sources, 1.0.0 {@link PluginData}
 *
 * When an artifact with plugins is added without specifying any parent artifact, the plugin is an universal plugin
 * and is usable for any artifact in the same namespace (or if the plugin artifact is in the system namespace,
 * then it is usable by any artifact in any namespace).
 * A special row will be stored for the universal plugin.
 * For example, when a user adds artifact mysql-driver-5.0.0 to the default namespace, the meta table will look like:
 *
 * table                             primary keys                                                  value
 * app_data                          system, ETLBatch, system, etlbatch, 3.1.0                    {@link AppData}
 * artifact_data                     default, custom-sources, 1.0.0                               {@link ArtifactData}
 * artifact_data                     system, etlbatch, 3.1.0                                      {@link ArtifactData}
 * artifact_data                     system, etlbatch-lib, 3.1.0                                  {@link ArtifactData}
 * plugin_data                       system, etlbatch, sink, stream, system, etlbatch-lib, 3.1.0  {@link PluginData}
 * plugin_data                       system, etlbatch, sink, table, system, etlbatch-lib, 3.1.0   {@link PluginData}
 * plugin_data                       system, etlbatch, source, db, default, custom-sources, 1.0.0 {@link PluginData}
 * universal_plugin_data             default, jdbc, yusql, default, mysql-driver, 5.0.0           {@link PluginData}
 *
 * With this schema we can perform a scan to look up AppClasses, a scan to look up plugins that extend a specific
 * artifact, and a scan to look up artifacts.
 *
 * In order to prevent deadlock if the storage backend is SQL, if a transaction needs to use multiple tables, the order
 * to use the table will be: artifact_data -> app_data -> plugin_data -> universal_plugin_data
 */
public class ArtifactStore {
  private static final String ARTIFACTS_PATH = "artifacts";

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(ArtifactRange.class, new ArtifactRangeCodec())
    .registerTypeAdapter(ApplicationClass.class, new ApplicationClassCodec())
    .registerTypeAdapter(Requirements.class, new RequirementsCodec())
    .create();

  private final LocationFactory locationFactory;
  private final NamespacePathLocator namespacePathLocator;
  private final Impersonator impersonator;
  private final Set<String> requirementBlacklist;
  private final TransactionRunner transactionRunner;

  @Inject
  ArtifactStore(CConfiguration cConf,
                NamespacePathLocator namespacePathLocator,
                LocationFactory locationFactory,
                Impersonator impersonator,
                TransactionRunner transactionRunner) {
    this.locationFactory = locationFactory;
    this.namespacePathLocator = namespacePathLocator;
    this.impersonator = impersonator;
    this.requirementBlacklist =
      new HashSet<>(cConf.getTrimmedStringCollection(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE))
        .stream().map(String::toLowerCase).collect(Collectors.toSet());
    this.transactionRunner = transactionRunner;
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
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE);
      List<ArtifactDetail> artifacts = Lists.newArrayList();
      Range scanRange = createArtifactScanRange(namespace);
      // TODO: CDAP-14636 add scan method without limit
      try (CloseableIterator<StructuredRow> iterator = table.scan(scanRange, Integer.MAX_VALUE)) {
        getArtifacts(iterator, Integer.MAX_VALUE, null, () -> artifacts);
      }
      return Collections.unmodifiableList(artifacts);
    }, IOException.class);
  }

  /**
   * Get all artifacts that match artifacts in the given ranges.
   *
   * @param range the range to match artifacts in
   * @param limit the limit number of the result
   * @param order the order of the result
   * @return an unmodifiable list of all artifacts that match the given ranges. If none exist, an empty list
   *         is returned
   */
  public List<ArtifactDetail> getArtifacts(ArtifactRange range, int limit, ArtifactSortOrder order) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getArtifacts(getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE), range, limit, order);
    });
  }

  private List<ArtifactDetail> getArtifacts(StructuredTable artifactDataTable, ArtifactRange range,
                                            int limit,
                                            ArtifactSortOrder order) throws IOException {
    Collection<Field<?>> keys =
      Arrays.asList(Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_NAMESPACE_FIELD, range.getNamespace()),
                    Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_NAME_FIELD, range.getName()));
    // here we have to query with Integer.MAX_VALUE since we want to sort the result
    try (CloseableIterator<StructuredRow> iterator = artifactDataTable.scan(Range.singleton(keys), Integer.MAX_VALUE)) {
      return getArtifacts(iterator, limit, order, range);
    }
  }

  private List<ArtifactDetail> getArtifacts(Iterator<StructuredRow> iterator, int limit,
                                            ArtifactSortOrder order,
                                            @Nullable ArtifactRange range) {
    if (!iterator.hasNext()) {
      return Collections.emptyList();
    }

    List<ArtifactDetail> result = (order == ArtifactSortOrder.UNORDERED)
      ? getArtifacts(iterator, limit, range, ArrayList::new)
      : getSortedArtifacts(iterator, limit, order, range, ArrayList::new);

    return Collections.unmodifiableList(result);
  }

  private List<ArtifactDetail> getArtifacts(Iterator<StructuredRow> iterator, int limit, @Nullable ArtifactRange range,
                                            Supplier<List<ArtifactDetail>> resultSupplier) {
    return collectArtifacts(iterator, range, limit,
                            Collector.of(resultSupplier, List::add, createUnsupportedCombiner()));
  }

  private List<ArtifactDetail> getSortedArtifacts(Iterator<StructuredRow> iterator, int limit,
                                                  ArtifactSortOrder order, @Nullable ArtifactRange range,
                                                  Supplier<List<ArtifactDetail>> resultSupplier) {
    // Create a priority queue for remembering the highest/lowest N
    return collectArtifacts(iterator, range, Integer.MAX_VALUE, Collector.of(
      () -> MinMaxPriorityQueue.orderedBy(Comparator.comparing(ArtifactDetail::getDescriptor)).create(),
      (queue, artifactDetail) -> {
        queue.add(artifactDetail);
        if (queue.size() > limit) {
          // For ascending order, remove the largest. For descending order, remove the smallest.
          if (order == ArtifactSortOrder.ASC) {
            queue.pollLast();
          } else {
            queue.pollFirst();
          }
        }
      }, createUnsupportedCombiner(),
      queue -> {
        List<ArtifactDetail> result = resultSupplier.get();
        while (!queue.isEmpty()) {
          if (order == ArtifactSortOrder.ASC) {
            result.add(queue.pollFirst());
          } else {
            result.add(queue.pollLast());
          }
        }
        return result;
      }));
  }

  private <T> BinaryOperator<T> createUnsupportedCombiner() {
    return (t, t2) -> {
      throw new UnsupportedOperationException("Combiner is not supported");
    };
  }

  private <A, R> R collectArtifacts(Iterator<StructuredRow> iterator, @Nullable ArtifactRange range, int limit,
                                    Collector<ArtifactDetail, A, R> collector) {

    // Includes artifacts with version matching the one from the given range
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
      .map(row -> Maps.immutableEntry(
        new ArtifactVersion(row.getString(StoreDefinition.ArtifactStore.ARTIFACT_VER_FIELD)), row))
      .filter(e -> range == null || range.versionIsInRange(e.getKey()))
      .limit(limit)
      .map(e -> {
        StructuredRow row = e.getValue();
        ArtifactKey artifactKey = ArtifactKey.fromRow(row);
        ArtifactData data = GSON.fromJson(row.getString(StoreDefinition.ArtifactStore.ARTIFACT_DATA_FIELD),
                                          ArtifactData.class);
        ArtifactMeta filteredArtifactMeta = filterPlugins(data.meta);
        ArtifactId artifactId = new ArtifactId(artifactKey.name, e.getKey(),
                                               artifactKey.namespace.equals(NamespaceId.SYSTEM.getNamespace()) ?
                                                 ArtifactScope.SYSTEM : ArtifactScope.USER);
        Location artifactLocation = Locations.getLocationFromAbsolutePath(locationFactory, data.getLocationPath());
        return new ArtifactDetail(new ArtifactDescriptor(artifactId, artifactLocation), filteredArtifactMeta);
      })
      .collect(collector);
  }

  /**
   * Get information about all versions of the given artifact.
   *
   * @param namespace the namespace to get artifacts from
   * @param artifactName the name of the artifact to get
   * @param limit the limit number of the result
   * @param order the order of the result
   * @return unmodifiable list of information about all versions of the given artifact
   * @throws ArtifactNotFoundException if no version of the given artifact exists
   * @throws IOException if there was an exception reading the artifact information from the metastore
   */
  public List<ArtifactDetail> getArtifacts(NamespaceId namespace,
                                           String artifactName,
                                           int limit,
                                           ArtifactSortOrder order) throws ArtifactNotFoundException, IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE);
      Collection<Field<?>> keys =
        Arrays.asList(Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_NAMESPACE_FIELD,
                                         namespace.getNamespace()),
                      Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_NAME_FIELD, artifactName));

      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.singleton(keys), Integer.MAX_VALUE)) {
        List<ArtifactDetail> artifacts = getArtifacts(iterator, limit, order, null);
        if (artifacts.isEmpty()) {
          throw new ArtifactNotFoundException(namespace, artifactName);
        }

        return artifacts;
      }
    }, ArtifactNotFoundException.class, IOException.class);
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
    ArtifactData artifactData = TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE);
      ArtifactCell artifactCell = new ArtifactCell(artifactId);
      Optional<StructuredRow> row = table.read(artifactCell.keys);
      if (!row.isPresent()) {
        throw new ArtifactNotFoundException(artifactId.toEntityId());
      }
      return GSON.fromJson(row.get().getString(StoreDefinition.ArtifactStore.ARTIFACT_DATA_FIELD), ArtifactData.class);
    }, IOException.class, ArtifactNotFoundException.class);

    try {
      Location artifactLocation = impersonator.doAs(artifactId.getNamespace().toEntityId(), () ->
        Locations.getLocationFromAbsolutePath(locationFactory, artifactData.getLocationPath()));
      ArtifactMeta artifactMeta = filterPlugins(artifactData.meta);
      return new ArtifactDetail(new ArtifactDescriptor(artifactId.toArtifactId(), artifactLocation), artifactMeta);
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
  public SortedMap<ArtifactDescriptor, List<ApplicationClass>> getApplicationClasses(NamespaceId namespace) {
    return TransactionRunners.run(transactionRunner, context -> {
      SortedMap<ArtifactDescriptor, List<ApplicationClass>> result = Maps.newTreeMap();
      StructuredTable table = getTable(context, StoreDefinition.ArtifactStore.APP_DATA_TABLE);
      Collection<Field<?>> keys = Collections.singleton(
        Fields.stringField(StoreDefinition.ArtifactStore.NAMESPACE_FIELD, namespace.getNamespace()));
      try (CloseableIterator<StructuredRow> iterator = table.scan(Range.singleton(keys), Integer.MAX_VALUE)) {
        while (iterator.hasNext()) {
          StructuredRow row = iterator.next();
          Map.Entry<ArtifactDescriptor, ApplicationClass> entry = extractApplicationClass(row);
          List<ApplicationClass> existingAppClasses = result.computeIfAbsent(entry.getKey(),
                                                                             k -> new ArrayList<>());
          existingAppClasses.add(entry.getValue());
        }
      }
      return Collections.unmodifiableSortedMap(result);
    });
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
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = getTable(context, StoreDefinition.ArtifactStore.APP_DATA_TABLE);

      SortedMap<ArtifactDescriptor, ApplicationClass> result = Maps.newTreeMap();
      Collection<Field<?>> keys = ImmutableList.of(
        Fields.stringField(StoreDefinition.ArtifactStore.NAMESPACE_FIELD, namespace.getNamespace()),
        Fields.stringField(StoreDefinition.ArtifactStore.CLASS_NAME_FIELD, className));
      try (CloseableIterator<StructuredRow> iterator = table.scan(Range.singleton(keys), Integer.MAX_VALUE)) {
        while (iterator.hasNext()) {
          StructuredRow row = iterator.next();
          Map.Entry<ArtifactDescriptor, ApplicationClass> entry = extractApplicationClass(row);
          result.put(entry.getKey(), entry.getValue());
        }
      }
      return Collections.unmodifiableSortedMap(result);
    });
  }

  /**
   * Get all plugin classes that extend the given parent artifact.
   * Results are returned as a sorted map from plugin artifact to plugins in that artifact.
   * Map entries are sorted by the artifact id of the plugin.
   *
   * @param namespace the namespace to search for plugins. The system namespace is always included
   * @param parentArtifactId the id of the artifact to find plugins for
   * @return an unmodifiable map of plugin artifact to plugin classes for all plugin classes accessible by the given
   *         artifact. The map will never be null. If there are no plugin classes, an empty map will be returned.
   * @throws ArtifactNotFoundException if the artifact to find plugins for does not exist
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPluginClasses(NamespaceId namespace,
                                                                          Id.Artifact parentArtifactId)
    throws ArtifactNotFoundException, IOException {
    return getPluginClasses(namespace, parentArtifactId, null);
  }

  /**
   * Get all plugin classes of the given type that extend the given parent artifact.
   * Results are returned as a map from plugin artifact to plugins in that artifact.
   *
   * @param namespace the namespace to search for plugins. The system namespace is always included
   * @param parentArtifactId the id of the artifact to find plugins for
   * @param type the type of plugin to look for or {@code null} for matching any type
   * @return an unmodifiable map of plugin artifact to plugin classes for all plugin classes accessible by the
   *         given artifact. The map will never be null. If there are no plugin classes, an empty map will be returned.
   * @throws ArtifactNotFoundException if the artifact to find plugins for does not exist
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPluginClasses(NamespaceId namespace,
                                                                          Id.Artifact parentArtifactId,
                                                                          @Nullable String type)
    throws ArtifactNotFoundException, IOException {

    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable artifactDataTable = getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE);
      SortedMap<ArtifactDescriptor, Set<PluginClass>> plugins =
        getPluginsInArtifact(artifactDataTable, parentArtifactId,
                             input -> (type == null || type.equals(input.getType())) && isAllowed(input));

      // Scan plugins
      StructuredTable pluginTable = getTable(context, StoreDefinition.ArtifactStore.PLUGIN_DATA_TABLE);
      try (CloseableIterator<StructuredRow> iterator =
             pluginTable.scan(createPluginScanRange(parentArtifactId, type), Integer.MAX_VALUE)) {
        while (iterator.hasNext()) {
          StructuredRow row = iterator.next();
          addPluginToMap(namespace, parentArtifactId, plugins, row);
        }
      }

      // Scan universal plugins
      StructuredTable uniPluginTable = getTable(context, StoreDefinition.ArtifactStore.UNIV_PLUGIN_DATA_TABLE);
      List<Range> ranges = Arrays.asList(
        createUniversalPluginScanRange(namespace.getNamespace(), type),
        createUniversalPluginScanRange(NamespaceId.SYSTEM.getNamespace(), type)
      );

      for (Range range : ranges) {
        try (CloseableIterator<StructuredRow> iterator = uniPluginTable.scan(range, Integer.MAX_VALUE)) {
          while (iterator.hasNext()) {
            StructuredRow row = iterator.next();
            addPluginToMap(namespace, parentArtifactId, plugins, row);
          }
        }
      }

      return Collections.unmodifiableSortedMap(plugins);
    }, ArtifactNotFoundException.class, IOException.class);
  }

  /**
   * Get all plugin classes of the given type and name that extend the given parent artifact.
   * Results are returned as a map from plugin artifact to plugins in that artifact.
   *
   * @param namespace the namespace to search for plugins. The system namespace is always included.
   * @param parentArtifactId the id of the artifact to find plugins for
   * @param type the type of plugin to look for
   * @param name the name of the plugin to look for
   * @param pluginPredicate the predicate for the plugins
   * @param limit the limit number of the result
   * @param order the order of the result
   * @return an unmodifiable map of plugin artifact to plugin classes of the given type and name, accessible by the
   *         given artifact. The map will never be null, and will never be empty.
   * @throws PluginNotExistsException if no plugin with the given type and name exists in the namespace
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public SortedMap<ArtifactDescriptor, PluginClass> getPluginClasses(
    NamespaceId namespace, Id.Artifact parentArtifactId, String type, String name,
    @Nullable Predicate<io.cdap.cdap.proto.id.ArtifactId> pluginPredicate,
    int limit, ArtifactSortOrder order) throws IOException, ArtifactNotFoundException, PluginNotExistsException {
    return getPluginClasses(namespace, new ArtifactRange(parentArtifactId.getNamespace().getId(),
                                                         parentArtifactId.getName(),
                                                         parentArtifactId.getVersion(), true,
                                                         parentArtifactId.getVersion(), true), type, name,
                            pluginPredicate, limit, order);
  }

  /**
   * Get all plugin classes of the given type and name that extend the given parent artifact.
   * Results are returned as a map from plugin artifact to plugins in that artifact.
   *
   * @param namespace the namespace to search for plugins. The system namespace is always included.
   * @param parentArtifactRange the parent artifact range to find plugins for
   * @param type the type of plugin to look for
   * @param name the name of the plugin to look for
   * @param pluginRange the predicate for the plugins
   * @param limit the limit number of the result
   * @param order the order of the result
   * @return an unmodifiable map of plugin artifact to plugin classes of the given type and name, accessible by the
   *         given artifact. The map will never be null, and will never be empty.
   * @throws PluginNotExistsException if no plugin with the given type and name exists in the namespace
   * @throws IOException if there was an exception reading metadata from the metastore
   */
  public SortedMap<ArtifactDescriptor, PluginClass> getPluginClasses(
    NamespaceId namespace, ArtifactRange parentArtifactRange, String type, String name,
    @Nullable final Predicate<io.cdap.cdap.proto.id.ArtifactId> pluginRange, int limit, ArtifactSortOrder order)
    throws IOException, ArtifactNotFoundException, PluginNotExistsException {

    SortedMap<ArtifactDescriptor, PluginClass> result = TransactionRunners.run(transactionRunner, context -> {
      StructuredTable artifactDataTable = getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE);
      List<ArtifactDetail> parentArtifactDetails = getArtifacts(artifactDataTable, parentArtifactRange,
                                                                Integer.MAX_VALUE, null);

      if (parentArtifactDetails.isEmpty()) {
        throw new ArtifactNotFoundException(parentArtifactRange.getNamespace(), parentArtifactRange.getName());
      }

      SortedMap<ArtifactDescriptor, PluginClass> plugins = order == ArtifactSortOrder.DESC ?
        new TreeMap<>(Collections.reverseOrder()) :
        new TreeMap<>();

      List<Id.Artifact> parentArtifacts = new ArrayList<>();
      for (ArtifactDetail parentArtifactDetail : parentArtifactDetails) {
        parentArtifacts.add(Id.Artifact.from(Id.Namespace.from(parentArtifactRange.getNamespace()),
                                             parentArtifactDetail.getDescriptor().getArtifactId()));

        Set<PluginClass> parentPlugins = parentArtifactDetail.getMeta().getClasses().getPlugins();
        for (PluginClass pluginClass : parentPlugins) {
          if (pluginClass.getName().equals(name) && pluginClass.getType().equals(type) && isAllowed(pluginClass)) {
            plugins.put(parentArtifactDetail.getDescriptor(), pluginClass);
            break;
          }
        }
      }

      // Add all plugins that extends from the given set of parents
      StructuredTable pluginTable = getTable(context, StoreDefinition.ArtifactStore.PLUGIN_DATA_TABLE);
      PluginKeyPrefix pluginKey = new PluginKeyPrefix(parentArtifactRange.getNamespace(),
                                                      parentArtifactRange.getName(), type, name);
      try (CloseableIterator<StructuredRow> iterator =
             pluginTable.scan(Range.singleton(pluginKey.keys), Integer.MAX_VALUE)) {
        addPluginsInRangeToMap(namespace, parentArtifacts, iterator, plugins, pluginRange, limit);
      }

      // Add all universal plugins
      StructuredTable uniPluginTable = getTable(context, StoreDefinition.ArtifactStore.UNIV_PLUGIN_DATA_TABLE);
      for (String ns : Arrays.asList(namespace.getNamespace(), NamespaceId.SYSTEM.getNamespace())) {
        UniversalPluginKeyPrefix universalPluginKey = new UniversalPluginKeyPrefix(ns, type, name);
        try (CloseableIterator<StructuredRow> iterator =
               uniPluginTable.scan(Range.singleton(universalPluginKey.keys), Integer.MAX_VALUE)) {
          addPluginsInRangeToMap(namespace, parentArtifacts, iterator, plugins, pluginRange, limit);
        }
      }

      return Collections.unmodifiableSortedMap(plugins);
    }, IOException.class, ArtifactNotFoundException.class);

    if (result.isEmpty()) {
      throw new PluginNotExistsException(new NamespaceId(parentArtifactRange.getNamespace()), type, name);
    }
    return result;
  }

  /**
   * Update artifact properties using an update function. Functions will receive an immutable map.
   *
   * @param artifactId the id of the artifact to add
   * @param updateFunction the function used to update existing properties
   * @throws ArtifactNotFoundException if the artifact does not exist
   * @throws IOException if there was an exception writing the properties to the metastore
   */
  public void updateArtifactProperties(Id.Artifact artifactId,
                                       Function<Map<String, String>, Map<String, String>> updateFunction)
    throws ArtifactNotFoundException, IOException {

    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable artifactDataTable = getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE);
      ArtifactCell artifactCell = new ArtifactCell(artifactId);
      Optional<StructuredRow> optional = artifactDataTable.read(artifactCell.keys);
      if (!optional.isPresent()) {
        throw new ArtifactNotFoundException(artifactId.toEntityId());
      }

      ArtifactData old = GSON.fromJson(optional.get().getString(StoreDefinition.ArtifactStore.ARTIFACT_DATA_FIELD),
                                       ArtifactData.class);
      ArtifactMeta updatedMeta = new ArtifactMeta(old.meta.getClasses(), old.meta.getUsableBy(),
                                                  updateFunction.apply(old.meta.getProperties()));
      ArtifactData updatedData =
        new ArtifactData(Locations.getLocationFromAbsolutePath(locationFactory, old.getLocationPath()),
                         updatedMeta);
      // write artifact metadata
      List<Field<?>> fields = ImmutableList.<Field<?>>builder()
        .addAll(artifactCell.keys)
        .add(Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_DATA_FIELD, GSON.toJson(updatedData)))
        .build();
      artifactDataTable.upsert(fields);
    }, ArtifactNotFoundException.class, IOException.class);
  }

  /**
   * Write the artifact and its metadata to the store. Once added, artifacts cannot be changed unless they are
   * snapshot versions.
   *
   * @param artifactId the id of the artifact to add
   * @param artifactMeta the metadata for the artifact
   * @param artifactContent the file containing the content of the artifact
   * @return detail about the newly added artifact
   * @throws ArtifactAlreadyExistsException if a non-snapshot version of the artifact already exists
   * @throws IOException if there was an exception persisting the artifact contents to the filesystem,
   *                     of persisting the artifact metadata to the metastore
   */
  public ArtifactDetail write(
    Id.Artifact artifactId, ArtifactMeta artifactMeta,
    File artifactContent, EntityImpersonator entityImpersonator) throws ArtifactAlreadyExistsException, IOException {

    // if we're not a snapshot version, check that the artifact doesn't exist already.
    if (!artifactId.getVersion().isSnapshot()) {
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE);
        ArtifactCell artifactCell = new ArtifactCell(artifactId);
        if (table.read(artifactCell.keys).isPresent()) {
          throw new ArtifactAlreadyExistsException(artifactId.toEntityId());
        }
      }, ArtifactAlreadyExistsException.class, IOException.class);
    }

    final Location destination;
    try {
      destination = copyFileToDestination(artifactId, artifactContent, entityImpersonator);
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }

    // now try and write the metadata for the artifact
    try {
      transactionRunner.run(context -> {
        // we have to check that the metadata doesn't exist again since somebody else may have written
        // the artifact while we were copying the artifact to the filesystem.
        StructuredTable artifactDataTable = getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE);
        ArtifactCell artifactCell = new ArtifactCell(artifactId);
        Optional<StructuredRow> optional = artifactDataTable.read(artifactCell.keys);
        boolean isSnapshot = artifactId.getVersion().isSnapshot();
        if (optional.isPresent() && !isSnapshot) {
          // non-snapshot artifacts are immutable. If there is existing metadata, stop here.
          throw new ArtifactAlreadyExistsException(artifactId.toEntityId());
        }

        ArtifactData data = new ArtifactData(destination, artifactMeta);
        // cleanup existing metadata if it exists and this is a snapshot
        // if we are overwriting a previous snapshot, need to clean up the old snapshot data
        // this means cleaning up the old jar, and deleting plugin and app rows.
        if (optional.isPresent()) {
          deleteMeta(context, artifactId,
                     GSON.fromJson(optional.get().getString(StoreDefinition.ArtifactStore.ARTIFACT_DATA_FIELD),
                                   ArtifactData.class));
        }
        // write artifact metadata
        writeMeta(context, artifactId, data);
      });

      return new ArtifactDetail(new ArtifactDescriptor(artifactId.toArtifactId(), destination), artifactMeta);
    } catch (TransactionException e) {
      destination.delete();
      // TODO: CDAP-14672 define TransactionConflictException for the SPI
      // should throw WriteConflictException(artifactId) on transaction conflict
      throw TransactionRunners.propagate(e, ArtifactAlreadyExistsException.class, IOException.class);
    }
  }

  private Location copyFileToDestination(final Id.Artifact artifactId,
                                         File artifactContent,
                                         EntityImpersonator entityImpersonator) throws Exception {
    return entityImpersonator.impersonate(() -> copyFile(artifactId, artifactContent));
  }

  private Location copyFile(Id.Artifact artifactId, File artifactContent) throws IOException {
    Location fileDirectory = namespacePathLocator.get(artifactId.getNamespace().toEntityId())
      .append(ARTIFACTS_PATH).append(artifactId.getName());
    Location destination = fileDirectory.append(artifactId.getVersion().getVersion()).getTempFile(".jar");
    Locations.mkdirsIfNotExists(fileDirectory);
    // write the file contents
    try (OutputStream destinationStream = destination.getOutputStream()) {
      Files.copy(artifactContent.toPath(), destinationStream);
    }
    return destination;
  }

  /**
   * Delete the specified artifact. Programs that use the artifact will no longer be able to start.
   *
   * @param artifactId the id of the artifact to delete
   * @throws IOException if there was an IO error deleting the metadata or the actual artifact
   */
  public void delete(final Id.Artifact artifactId) throws ArtifactNotFoundException, IOException {

    // delete everything in a transaction
    TransactionRunners.run(transactionRunner, context -> {
      // first look up details to get plugins and apps in the artifact
      StructuredTable artifactDataTable = getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE);
      ArtifactCell artifactCell = new ArtifactCell(artifactId);
      Optional<StructuredRow> optional = artifactDataTable.read(artifactCell.keys);
      if (!optional.isPresent()) {
        throw new ArtifactNotFoundException(artifactId.toEntityId());
      }
      deleteMeta(context, artifactId,
                 GSON.fromJson(optional.get().getString(StoreDefinition.ArtifactStore.ARTIFACT_DATA_FIELD),
                               ArtifactData.class));
    }, IOException.class, ArtifactNotFoundException.class);
  }

  /**
   * Clear all data in the given namespace. Used only in unit tests.
   *
   * @param namespace the namespace to delete data in
   * @throws IOException if there was some problem deleting the data
   */
  @VisibleForTesting
  void clear(final NamespaceId namespace) throws IOException {
    final Id.Namespace namespaceId = Id.Namespace.fromEntityId(namespace);
    namespacePathLocator.get(namespace).append(ARTIFACTS_PATH).delete(true);

    TransactionRunners.run(transactionRunner, context -> {
      // delete all rows about artifacts in the namespace
      StructuredTable artifactDataTable = getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE);
      Range artifactScanRange = createArtifactScanRange(namespace);
      deleteRangeFromTable(artifactDataTable, artifactScanRange);

      // delete all rows about artifacts in the namespace and the plugins they have access to
      StructuredTable pluginDataTable = getTable(context, StoreDefinition.ArtifactStore.PLUGIN_DATA_TABLE);
      Collection<Field<?>> pluginKey =
        Collections.singleton(Fields.stringField(StoreDefinition.ArtifactStore.PARENT_NAMESPACE_FIELD,
                                                 namespace.getNamespace()));
      deleteRangeFromTable(pluginDataTable, Range.singleton(pluginKey));

      // delete all rows about universal plugins
      StructuredTable univPluginsDataTable  = getTable(context, StoreDefinition.ArtifactStore.UNIV_PLUGIN_DATA_TABLE);
      deleteRangeFromTable(univPluginsDataTable,
                           createUniversalPluginScanRange(namespace.getNamespace(), null));

      // delete app classes in this namespace
      StructuredTable appClassTable = getTable(context, StoreDefinition.ArtifactStore.APP_DATA_TABLE);
      deleteRangeFromTable(appClassTable, createAppClassRange(namespace));

      // delete plugins in this namespace from system artifacts
      // for example, if there was an artifact in this namespace that extends a system artifact
      Collection<Field<?>> systemPluginKey =
        Collections.singleton(Fields.stringField(StoreDefinition.ArtifactStore.PARENT_NAMESPACE_FIELD,
                                                 Id.Namespace.SYSTEM.getId()));
      try (CloseableIterator<StructuredRow> iterator =
             pluginDataTable.scan(Range.singleton(systemPluginKey), Integer.MAX_VALUE)) {
        while (iterator.hasNext()) {
          StructuredRow row = iterator.next();

          // if the plugin artifact is in the namespace we're deleting, delete this column.
          if (namespaceId.getId().equals(row.getString(StoreDefinition.ArtifactStore.ARTIFACT_NAMESPACE_FIELD))) {
            pluginDataTable.delete(concatFields(PluginKeyPrefix.fromRow(row), ArtifactCell.fromRow(row)));
          }
        }
      }
    }, IOException.class);
  }

  private void deleteRangeFromTable(StructuredTable table, Range range) throws IOException {
    try (CloseableIterator<StructuredRow> iterator = table.scan(range, Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        table.delete(row.getPrimaryKeys());
      }
    }
  }

  private Map.Entry<ArtifactDescriptor, ApplicationClass> extractApplicationClass(StructuredRow row) {
    Id.Artifact artifactId =
      Id.Artifact.from(Id.Namespace.from(row.getString(StoreDefinition.ArtifactStore.ARTIFACT_NAMESPACE_FIELD)),
                       row.getString(StoreDefinition.ArtifactStore.ARTIFACT_NAME_FIELD),
                       row.getString(StoreDefinition.ArtifactStore.ARTIFACT_VER_FIELD));
    AppData appData = GSON.fromJson(row.getString(StoreDefinition.ArtifactStore.APP_DATA_FIELD), AppData.class);
    ArtifactDescriptor artifactDescriptor = new ArtifactDescriptor(
      artifactId.toArtifactId(),
      Locations.getLocationFromAbsolutePath(locationFactory, appData.getArtifactLocationPath()));
    return new AbstractMap.SimpleEntry<>(artifactDescriptor, appData.appClass);
  }

  // write a new artifact snapshot and clean up the old snapshot data
  private void writeMeta(StructuredTableContext context, Id.Artifact artifactId,
                         ArtifactData data) throws IOException {
    // write to artifact data table
    StructuredTable artifactDataTable = getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE);
    ArtifactCell artifactCell = new ArtifactCell(artifactId);
    artifactDataTable.upsert(
      concatFields(artifactCell.keys, Collections.singleton(
        Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_DATA_FIELD, GSON.toJson(data)))));


    ArtifactClasses classes = data.meta.getClasses();
    Location artifactLocation = Locations.getLocationFromAbsolutePath(locationFactory, data.getLocationPath());

    // write appClass metadata
    StructuredTable appTable = getTable(context, StoreDefinition.ArtifactStore.APP_DATA_TABLE);
    ArtifactCell artifactkeys = new ArtifactCell(artifactId);
    for (ApplicationClass appClass : classes.getApps()) {
      // a:{namespace}:{classname}
      AppClassKey appClassKey = new AppClassKey(artifactId.getNamespace().toEntityId(), appClass.getClassName());
      Field<String> appDataField = Fields.stringField(StoreDefinition.ArtifactStore.APP_DATA_FIELD,
                                                      GSON.toJson(new AppData(appClass, artifactLocation)));
      appTable.upsert(concatFields(appClassKey.keys, artifactkeys.keys, Collections.singleton(appDataField)));
    }

    // write pluginClass metadata, we loop twice to only access to one table at a time to prevent deadlock
    StructuredTable pluginTable = getTable(context, StoreDefinition.ArtifactStore.PLUGIN_DATA_TABLE);
    for (PluginClass pluginClass : classes.getPlugins()) {
      // write metadata for each artifact this plugin extends
      for (ArtifactRange artifactRange : data.meta.getUsableBy()) {
        PluginKeyPrefix pluginKey = new PluginKeyPrefix(artifactRange.getNamespace(),
                                                        artifactRange.getName(), pluginClass.getType(),
                                                        pluginClass.getName());

        Field<String> pluginDataField = Fields.stringField(StoreDefinition.ArtifactStore.PLUGIN_DATA_FIELD,
                                                           GSON.toJson(new PluginData(pluginClass, artifactLocation,
                                                                                      artifactRange)));
        pluginTable.upsert(concatFields(pluginKey.keys, artifactkeys.keys, Collections.singleton(pluginDataField)));
      }
    }

    // write universal plugin class metadata
    StructuredTable uniPluginTable = getTable(context, StoreDefinition.ArtifactStore.UNIV_PLUGIN_DATA_TABLE);
    for (PluginClass pluginClass : classes.getPlugins()) {
      // If the artifact is deployed without any parent, add a special row to indicate that it can be used
      // by any other artifact in the same namespace.
      if (data.meta.getUsableBy().isEmpty()) {
        // Write a special entry for plugin that doesn't have parent, which means any artifact can use it
        UniversalPluginKeyPrefix pluginKey = new UniversalPluginKeyPrefix(artifactId.getNamespace().getId(),
                                                                          pluginClass.getType(), pluginClass.getName());
        Field<String> pluginDataField = Fields.stringField(StoreDefinition.ArtifactStore.PLUGIN_DATA_FIELD,
                                                           GSON.toJson(new PluginData(pluginClass, artifactLocation,
                                                                                      null)));
        uniPluginTable.upsert(concatFields(pluginKey.keys, artifactkeys.keys, Collections.singleton(pluginDataField)));
      }
    }
  }

  private Collection<Field<?>> concatFields(Collection<Field<?>> fields1, Collection<Field<?>> fields2) {
    List<Field<?>> allFields = new ArrayList<>(fields1);
    allFields.addAll(fields2);
    return allFields;
  }

  private Collection<Field<?>> concatFields(Collection<Field<?>> fields1, Collection<Field<?>> fields2,
                                            Collection<Field<?>> fields3) {
    List<Field<?>> allFields = new ArrayList<>(fields1);
    allFields.addAll(fields2);
    allFields.addAll(fields3);
    return allFields;
  }

  private void deleteMeta(StructuredTableContext context, Id.Artifact artifactId, ArtifactData oldMeta)
    throws IOException {
    // delete old artifact data
    StructuredTable artifactTable = getTable(context, StoreDefinition.ArtifactStore.ARTIFACT_DATA_TABLE);
    ArtifactCell artifactCell = new ArtifactCell(artifactId);
    artifactTable.delete(artifactCell.keys);

    // delete old appclass metadata
    StructuredTable appClassTable = getTable(context, StoreDefinition.ArtifactStore.APP_DATA_TABLE);
    for (ApplicationClass appClass : oldMeta.meta.getClasses().getApps()) {
      AppClassKey appClassKey = new AppClassKey(artifactId.getNamespace().toEntityId(), appClass.getClassName());
      deleteRangeFromTable(appClassTable, Range.singleton(appClassKey.keys));
    }

    // delete old plugins, we loop twice to only access to one table at a time to prevent deadlock
    StructuredTable pluginDataTable = getTable(context, StoreDefinition.ArtifactStore.PLUGIN_DATA_TABLE);
    for (PluginClass pluginClass : oldMeta.meta.getClasses().getPlugins()) {
      // delete metadata for each artifact this plugin extends
      for (ArtifactRange artifactRange : oldMeta.meta.getUsableBy()) {
        // these four fields are prefixes of the plugin table primary keys
        PluginKeyPrefix pluginKey = new PluginKeyPrefix(artifactRange.getNamespace(),
                                                        artifactRange.getName(), pluginClass.getType(),
                                                        pluginClass.getName());
        pluginDataTable.delete(concatFields(pluginKey.keys, artifactCell.keys));
      }
    }

    // Delete the universal plugin row
    StructuredTable uniPluginTable = getTable(context, StoreDefinition.ArtifactStore.UNIV_PLUGIN_DATA_TABLE);
    for (PluginClass pluginClass : oldMeta.meta.getClasses().getPlugins()) {
      if (oldMeta.meta.getUsableBy().isEmpty()) {
        UniversalPluginKeyPrefix pluginKey = new UniversalPluginKeyPrefix(artifactId.getNamespace().getId(),
                                                                          pluginClass.getType(), pluginClass.getName());
        uniPluginTable.delete(concatFields(pluginKey.keys, artifactCell.keys));
      }
    }

    // delete the old jar file
    try {
      new EntityImpersonator(artifactId.toEntityId(), impersonator).impersonate(() -> {
        Locations.getLocationFromAbsolutePath(locationFactory, oldMeta.getLocationPath()).delete();
        return null;
      });
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      // this should not happen
      throw Throwables.propagate(e);
    }
  }

  private StructuredTable getTable(StructuredTableContext context, StructuredTableId id) {
    try {
      return context.getTable(id);
    } catch (TableNotFoundException e) {
      // this should not be translated to a NotFoundException, as this type of error is an internal error
      // and not an entity not found error
      throw new IllegalStateException(
        String.format("System table '%s' could not be found. "
                        + "Please check that your environment is properly set up.", id), e);
    }
  }

  private SortedMap<ArtifactDescriptor, Set<PluginClass>> getPluginsInArtifact(StructuredTable artifactDataTable,
                                                                               Id.Artifact artifactId,
                                                                               Predicate<PluginClass> filter)
    throws ArtifactNotFoundException, IOException {
    SortedMap<ArtifactDescriptor, Set<PluginClass>> result = new TreeMap<>();

    // Make sure the artifact exists
    ArtifactCell artifactCell = new ArtifactCell(artifactId);
    Optional<StructuredRow> row = artifactDataTable.read(artifactCell.keys);
    if (!row.isPresent()) {
      throw new ArtifactNotFoundException(artifactId.toEntityId());
    }
    // include any plugin classes that are inside the artifact itself and is accepted by the filter
    ArtifactData artifactData = GSON.fromJson(row.get().getString(StoreDefinition.ArtifactStore.ARTIFACT_DATA_FIELD),
                                              ArtifactData.class);
    Set<PluginClass> plugins = artifactData.meta.getClasses().getPlugins().stream()
      .filter(filter).collect(Collectors.toCollection(LinkedHashSet::new));

    if (!plugins.isEmpty()) {
      Location location = Locations.getLocationFromAbsolutePath(locationFactory, artifactData.getLocationPath());
      ArtifactDescriptor descriptor = new ArtifactDescriptor(artifactId.toArtifactId(), location);
      result.put(descriptor, plugins);
    }
    return result;
  }

  // this method examines the plugin in the given row and checks if they extend the given parent artifact
  // and are from an artifact in the given namespace.
  // if so, information about the plugin artifact and the plugin details are added to the given map.
  private void addPluginToMap(NamespaceId namespace, Id.Artifact parentArtifactId,
                              SortedMap<ArtifactDescriptor, Set<PluginClass>> map,
                              StructuredRow row) {
    ImmutablePair<ArtifactDescriptor, PluginClass> pluginEntry = getPluginEntry(namespace, parentArtifactId, row);
    if (pluginEntry != null && isAllowed(pluginEntry.getSecond())) {
      map.computeIfAbsent(pluginEntry.getFirst(), k -> new HashSet<>()).add(pluginEntry.getSecond());
    }
  }

  /**
   * Decode the PluginClass from the table column if it is from an artifact in the given namespace and
   * extends the given parent artifact. If the plugin's artifact is not in the given namespace, or it does not
   * extend the given parent artifact, return null.
   */
  @Nullable
  private ImmutablePair<ArtifactDescriptor, PluginClass> getPluginEntry(
    NamespaceId namespace, Id.Artifact parentArtifactId, StructuredRow row) {
    ImmutablePair<ArtifactDescriptor, PluginData> pluginPair = getPlugin(row, artifactId -> {
      NamespaceId namespaceId = artifactId.getNamespaceId();
      return NamespaceId.SYSTEM.equals(namespaceId) || namespace.equals(namespaceId);
    });

    if (pluginPair == null) {
      return null;
    }

    PluginData pluginData = pluginPair.getSecond();
    // filter out plugins that don't extend this version of the parent artifact
    if (pluginData.isUsableBy(parentArtifactId.toEntityId())) {
      return ImmutablePair.of(pluginPair.getFirst(), pluginData.pluginClass);
    }
    return null;
  }

  private void addPluginsInRangeToMap(final NamespaceId namespace, List<Id.Artifact> parentArtifacts,
                                      Iterator<StructuredRow> iterator,
                                      SortedMap<ArtifactDescriptor, PluginClass> plugins,
                                      @Nullable Predicate<io.cdap.cdap.proto.id.ArtifactId> range,
                                      int limit) {
    // if predicate is null,
    // filter out plugins whose artifacts are not in the system namespace and not in this namespace
    range = range != null
      ? range
      : input -> NamespaceId.SYSTEM.equals(input.getParent()) || input.getParent().equals(namespace);

    while (iterator.hasNext()) {
      StructuredRow row = iterator.next();
      ImmutablePair<ArtifactDescriptor, PluginData> pluginPair = getPlugin(row, range);
      if (pluginPair == null) {
        continue;
      }

      PluginData pluginData = pluginPair.getSecond();
      // filter out plugins that don't extend this version of the parent artifact
      for (Id.Artifact parentArtifactId : parentArtifacts) {
        if (pluginData.isUsableBy(parentArtifactId.toEntityId()) && isAllowed(pluginData.pluginClass)) {
          plugins.put(pluginPair.getFirst(), pluginData.pluginClass);
          break;
        }
      }
      if (limit < plugins.size()) {
        plugins.remove(plugins.lastKey());
      }
    }
  }

  @Nullable
  private ImmutablePair<ArtifactDescriptor, PluginData> getPlugin(StructuredRow row,
                                                                  Predicate<io.cdap.cdap.proto.id.ArtifactId> range) {
    // column is the artifact namespace, name, and version. value is the serialized PluginData
    Id.Namespace artifactNamespace =
      Id.Namespace.from(row.getString(StoreDefinition.ArtifactStore.ARTIFACT_NAMESPACE_FIELD));
    Id.Artifact artifactId =
      Id.Artifact.from(artifactNamespace, row.getString(StoreDefinition.ArtifactStore.ARTIFACT_NAME_FIELD),
                       row.getString(StoreDefinition.ArtifactStore.ARTIFACT_VER_FIELD));

    if (!range.test(artifactId.toEntityId())) {
      return null;
    }
    PluginData pluginData = GSON.fromJson(row.getString(StoreDefinition.ArtifactStore.PLUGIN_DATA_FIELD),
                                          PluginData.class);
    return ImmutablePair.of(new ArtifactDescriptor(
      artifactId.toArtifactId(),
      Locations.getLocationFromAbsolutePath(locationFactory, pluginData.getArtifactLocationPath())), pluginData);

  }

  private Range createArtifactScanRange(NamespaceId namespace) {
    Field<String> stringField = Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_NAMESPACE_FIELD,
                                                   namespace.getNamespace());
    return Range.singleton(Collections.singleton(stringField));
  }

  private Range createPluginScanRange(Id.Artifact parentArtifactId, @Nullable String type) {
    List<Field<?>> keys = new ArrayList<>();
    keys.add(Fields.stringField(StoreDefinition.ArtifactStore.PARENT_NAMESPACE_FIELD,
                                parentArtifactId.getNamespace().getId()));
    keys.add(Fields.stringField(StoreDefinition.ArtifactStore.PARENT_NAME_FIELD, parentArtifactId.getName()));
    if (type != null) {
      keys.add(Fields.stringField(StoreDefinition.ArtifactStore.PLUGIN_TYPE_FIELD, type));
    }
    return Range.singleton(keys);
  }

  private Range createUniversalPluginScanRange(String namespace, @Nullable String type) {
    List<Field<?>> keys = new ArrayList<>();
    keys.add(Fields.stringField(StoreDefinition.ArtifactStore.NAMESPACE_FIELD, namespace));
    if (type != null) {
      keys.add(Fields.stringField(StoreDefinition.ArtifactStore.PLUGIN_TYPE_FIELD, type));
    }
    return Range.singleton(keys);
  }

  private Range createAppClassRange(NamespaceId namespace) {
    return Range.singleton(Collections.singleton(Fields.stringField(StoreDefinition.ArtifactStore.NAMESPACE_FIELD,
                                                                    namespace.getNamespace())));
  }

  /**
   * Filters the plugins contained in the {@link ArtifactMeta} by using {@link #isAllowed(PluginClass)}
   *
   * @param artifactMeta the artifact meta
   * @return the {@link ArtifactMeta} containing only the plugins which are not excluded
   */
  private ArtifactMeta filterPlugins(ArtifactMeta artifactMeta) {
    ArtifactClasses classes = artifactMeta.getClasses();
    Set<PluginClass> filteredPlugins = classes.getPlugins().stream()
      .filter(this::isAllowed).collect(Collectors.toSet());
    ArtifactClasses filteredClasses = ArtifactClasses.builder()
      .addApps(classes.getApps())
      .addPlugins(filteredPlugins)
      .build();
    return new ArtifactMeta(filteredClasses, artifactMeta.getUsableBy(), artifactMeta.getProperties());
  }

  /**
   * Checks whether the plugin is excluded from being displayed/used and should be filtered out. The exclusion is
   * determined by the requirements of the plugin and the configuration for
   * {@link Constants#REQUIREMENTS_DATASET_TYPE_EXCLUDE}
   *
   * @param pluginClass the plugins class to check
   * @return true if the plugin should not be excluded and is allowed else false
   */
  private boolean isAllowed(PluginClass pluginClass) {
    // if a plugin has any requirement which is marked excluded in the config then the plugin should not be allowed.
    // currently we only allow dataset type requirements
    return pluginClass.getRequirements().getDatasetTypes().stream().noneMatch(requirementBlacklist::contains);
  }

  private static class AppClassKey {
    private final Collection<Field<?>> keys;

    AppClassKey(NamespaceId namespace, String className) {
      this.keys = Arrays.asList(
        Fields.stringField(StoreDefinition.ArtifactStore.NAMESPACE_FIELD, namespace.getNamespace()),
        Fields.stringField(StoreDefinition.ArtifactStore.CLASS_NAME_FIELD, className)
      );
    }
  }

  private static class PluginKeyPrefix {
    private final Collection<Field<?>> keys;

    private PluginKeyPrefix(String parentArtifactNamespace, String parentArtifactName, String type, String name) {
      this.keys = Arrays.asList(
        Fields.stringField(StoreDefinition.ArtifactStore.PARENT_NAMESPACE_FIELD, parentArtifactNamespace),
        Fields.stringField(StoreDefinition.ArtifactStore.PARENT_NAME_FIELD, parentArtifactName),
        Fields.stringField(StoreDefinition.ArtifactStore.PLUGIN_TYPE_FIELD, type),
        Fields.stringField(StoreDefinition.ArtifactStore.PLUGIN_NAME_FIELD, name)
      );
    }

    private static Collection<Field<?>> fromRow(StructuredRow row) {
      return Arrays.asList(
        Fields.stringField(StoreDefinition.ArtifactStore.PARENT_NAMESPACE_FIELD,
                           row.getString(StoreDefinition.ArtifactStore.PARENT_NAMESPACE_FIELD)),
        Fields.stringField(StoreDefinition.ArtifactStore.PARENT_NAME_FIELD,
                           row.getString(StoreDefinition.ArtifactStore.PARENT_NAME_FIELD)),
        Fields.stringField(StoreDefinition.ArtifactStore.PLUGIN_TYPE_FIELD,
                           row.getString(StoreDefinition.ArtifactStore.PLUGIN_TYPE_FIELD)),
        Fields.stringField(StoreDefinition.ArtifactStore.PLUGIN_NAME_FIELD,
                           row.getString(StoreDefinition.ArtifactStore.PLUGIN_NAME_FIELD))
      );
    }
  }

  private static final class UniversalPluginKeyPrefix {
    Collection<Field<?>> keys;

    private UniversalPluginKeyPrefix(String namespace, String type, String name) {
      this.keys = Arrays.asList(
        Fields.stringField(StoreDefinition.ArtifactStore.NAMESPACE_FIELD, namespace),
        Fields.stringField(StoreDefinition.ArtifactStore.PLUGIN_TYPE_FIELD, type),
        Fields.stringField(StoreDefinition.ArtifactStore.PLUGIN_NAME_FIELD, name)
      );
    }
  }

  // utilities for creating and parsing row keys for artifacts. Keys are of the form 'r:{namespace}:{artifact-name}'
  private static class ArtifactKey {
    private final String namespace;
    private final String name;

    private ArtifactKey(String namespace, String name) {
      this.namespace = namespace;
      this.name = name;
    }

    private static ArtifactKey fromRow(StructuredRow row) {
      return new ArtifactKey(row.getString(StoreDefinition.ArtifactStore.ARTIFACT_NAMESPACE_FIELD),
                             row.getString(StoreDefinition.ArtifactStore.ARTIFACT_NAME_FIELD));
    }
  }

  private static class ArtifactCell {
    private final Collection<Field<?>> keys;

    private ArtifactCell(Id.Artifact artifactId) {
      this.keys = Arrays.asList(
        Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_NAMESPACE_FIELD, artifactId.getNamespace().getId()),
        Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_NAME_FIELD, artifactId.getName()),
        Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_VER_FIELD, artifactId.getVersion().getVersion()));
    }

    private static Collection<Field<?>> fromRow(StructuredRow row) {
      return Arrays.asList(
        Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_NAMESPACE_FIELD,
                           row.getString(StoreDefinition.ArtifactStore.ARTIFACT_NAMESPACE_FIELD)),
        Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_NAME_FIELD,
                           row.getString(StoreDefinition.ArtifactStore.ARTIFACT_NAME_FIELD)),
        Fields.stringField(StoreDefinition.ArtifactStore.ARTIFACT_VER_FIELD,
                           row.getString(StoreDefinition.ArtifactStore.ARTIFACT_VER_FIELD))
      );
    }
  }

  // Data that will be stored for an artifact. Same as ArtifactDetail, expected without the id since that is redundant.
  private static class ArtifactData {
    // For Backward Compatibility
    private final URI locationURI;
    private final String locationPath;
    private final ArtifactMeta meta;

    ArtifactData(Location location, ArtifactMeta meta) {
      this.locationURI = null;
      this.locationPath = location.toURI().getPath();
      this.meta = meta;
    }

    String getLocationPath() {
      return locationPath == null ? locationURI.getPath() : locationPath;
    }
  }

  // Data that will be stored for a plugin.
  private static class PluginData {
    private final PluginClass pluginClass;
    // URI For Backward Compatibility
    private final URI artifactLocationURI;
    private final String artifactLocationPath;
    @Nullable
    private final ArtifactRange usableBy;

    PluginData(PluginClass pluginClass, Location artifactLocation, @Nullable ArtifactRange usableBy) {
      this.pluginClass = pluginClass;
      this.usableBy = usableBy;
      this.artifactLocationURI = null;
      this.artifactLocationPath = artifactLocation.toURI().getPath();
    }

    String getArtifactLocationPath() {
      return artifactLocationPath == null ? artifactLocationURI.getPath() : artifactLocationPath;
    }

    boolean isUsableBy(io.cdap.cdap.proto.id.ArtifactId artifactId) {
      if (usableBy == null) {
        return true;
      }
      return usableBy.getNamespace().equals(artifactId.getNamespace())
        && usableBy.getName().equals(artifactId.getArtifact())
        && usableBy.versionIsInRange(new ArtifactVersion(artifactId.getVersion()));
    }
  }

  // Data that will be stored for an application class.
  private static class AppData {
    private final ApplicationClass appClass;
    // URI For Backward Compatibility
    private final URI artifactLocationURI;
    private final String artifactLocationPath;

    AppData(ApplicationClass appClass, Location artifactLocation) {
      this.appClass = appClass;
      this.artifactLocationURI = null;
      this.artifactLocationPath = artifactLocation.toURI().getPath();
    }

    String getArtifactLocationPath() {
      return artifactLocationPath == null ? artifactLocationURI.getPath() : artifactLocationPath;
    }
  }
}
