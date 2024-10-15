/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.InstanceConflictException;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.ObjectMappedTable;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetProperties;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.file.FileSetDataset;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Has handy methods for dealing with Datasets.
 * todo: once we have couple methods, refactor out from "util" into smth more sensible
 */
public final class DatasetsUtil {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetsUtil.class);

  private DatasetsUtil() {
  }

  /**
   * Gets instance of {@link Dataset}, while add instance to {@link DatasetFramework} and creating
   * the physical data set if that one doesn't exist.
   */
  public static <T extends Dataset> T getOrCreateDataset(DatasetFramework datasetFramework,
      DatasetId datasetInstanceId, String typeName,
      DatasetProperties props,
      @Nullable Map<String, String> arguments)
      throws DatasetManagementException, IOException, UnauthorizedException {
    createIfNotExists(datasetFramework, datasetInstanceId, typeName, props);
    if (arguments == null) {
      arguments = Collections.emptyMap();
    }
    return datasetFramework.getDataset(datasetInstanceId, arguments, null);
  }

  /**
   * Gets a {@link Dataset} instance through the given {@link DatasetContext}. If the dataset
   * doesn't exist, it will be created using the given {@link DatasetFramework}.
   */
  public static <T extends Dataset> T getOrCreateDataset(DatasetContext datasetContext,
      DatasetFramework datasetFramework,
      DatasetId datasetId,
      String datasetTypename,
      DatasetProperties datasetProperties)
      throws DatasetManagementException, IOException, UnauthorizedException {

    try {
      return datasetContext.getDataset(datasetId.getNamespace(), datasetId.getDataset());
    } catch (DatasetInstantiationException e) {
      createIfNotExists(datasetFramework, datasetId, datasetTypename, datasetProperties);
      return datasetContext.getDataset(datasetId.getNamespace(), datasetId.getDataset());
    }
  }

  /**
   * Gets a {@link Dataset} instance through the given {@link DatasetContext}. If the dataset
   * doesn't exist, it will be created using the given {@link DatasetFramework}.
   */
  public static <T extends Dataset> T getOrCreateDataset(DatasetContext datasetContext,
      DatasetFramework datasetFramework,
      DatasetId datasetId,
      String datasetTypename,
      Supplier<DatasetProperties> datasetPropertiesSupplier)
      throws DatasetManagementException, IOException, UnauthorizedException {

    try {
      return datasetContext.getDataset(datasetId.getNamespace(), datasetId.getDataset());
    } catch (DatasetInstantiationException e) {
      createIfNotExists(datasetFramework, datasetId, datasetTypename,
          datasetPropertiesSupplier.get());
      return datasetContext.getDataset(datasetId.getNamespace(), datasetId.getDataset());
    }
  }

  /**
   * Creates instance of the data set if not exists
   */
  public static void createIfNotExists(DatasetFramework datasetFramework,
      DatasetId datasetInstanceId, String typeName,
      DatasetProperties props)
      throws DatasetManagementException, IOException, UnauthorizedException {

    if (!datasetFramework.hasInstance(datasetInstanceId)) {
      try {
        datasetFramework.addInstance(typeName, datasetInstanceId, props);
      } catch (InstanceConflictException e) {
        // Do nothing: someone created this instance in between, just continuing
      } catch (DatasetManagementException e) {
        LOG.error("Could NOT add dataset instance {} of type {} with props {}",
            datasetInstanceId, typeName, props, e);
        throw Throwables.propagate(e);
      }
    }
  }


  /**
   * For a dataset spec that does not contain the original properties, we attempt to reconstruct
   * them from the properties at the top-level of the spec. For most datasets, these will be
   * identical, however, there are a few known dataset types whose {@link
   * DatasetDefinition#configure(String, DatasetProperties)} method adds additional properties. As
   * of release 3.3, the set of built-in such dataset types is known. Any dataset created or
   * reconfigured beginning with 3.4 will have the original properties stored in the spec.
   *
   * @param spec a dataset spec that does not contain the original properties
   * @return the input spec if it is null or if it has original properties; otherwise a spec that
   *     has the original properties with which the dataset was created or reconfigured, at best
   *     effort.
   */
  @VisibleForTesting
  public static DatasetSpecification fixOriginalProperties(@Nullable DatasetSpecification spec) {
    if (spec == null || spec.getOriginalProperties() != null) {
      return spec;
    }
    Map<String, String> props = new TreeMap<>(spec.getProperties());
    if (!props.isEmpty()) {
      String type = spec.getType();

      // file sets add a fileset version indicating how to handle absolute base paths
      if (FileSet.class.getName().equals(type) || "fileSet".equals(type)) {
        props.remove(FileSetDataset.FILESET_VERSION_PROPERTY);

        // TPFS adds the partitioning
      } else if (TimePartitionedFileSet.class.getName().equals(type)
          || "timePartitionedFileSet".equals(type)) {
        props.remove(PartitionedFileSetProperties.PARTITIONING_FIELDS);
        for (String key : spec.getProperties().keySet()) {
          if (key.startsWith(PartitionedFileSetProperties.PARTITIONING_FIELD_PREFIX)) {
            props.remove(key);
          }
        }

        // ObjectMappedTable adds the table schema and its row field name
      } else if (ObjectMappedTable.class.getName().endsWith(type) || "objectMappedTable".equals(
          type)) {
        props.remove(Table.PROPERTY_SCHEMA);
        props.remove(Table.PROPERTY_SCHEMA_ROW_FIELD);
      }
    }
    return spec.setOriginalProperties(props);
  }

  public static boolean isUserDataset(DatasetId datasetInstanceId) {
    return !NamespaceId.SYSTEM.equals(datasetInstanceId.getParent())
        && !isSystemDatasetInUserNamespace(datasetInstanceId);
  }

  public static boolean isSystemDatasetInUserNamespace(DatasetId datasetInstanceId) {
    return !NamespaceId.SYSTEM.equals(datasetInstanceId.getParent())
        && ("system.queue.config".equals(datasetInstanceId.getEntityName())
        || datasetInstanceId.getEntityName().startsWith("system.sharded.queue")
        || datasetInstanceId.getEntityName().startsWith("system.queue")
        || datasetInstanceId.getEntityName().startsWith("system.stream"));
  }

  /**
   * Returns whether or not the dataset defined in the given specification is transactional.
   * Defaults to true. Note that this should be in TableProperties, but because we do not expose
   * this setting through the API currently, keeping it here for now. See
   * https://cdap.atlassian.net/browse/CDAP-1193 for additional information.
   */
  public static boolean isTransactional(Map<String, String> props) {
    return !"true".equalsIgnoreCase(props.get(Constants.Dataset.TABLE_TX_DISABLED));
  }
}
