/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.file.FileSetDataset;
import co.cask.cdap.data2.metadata.lineage.LineageDataset;
import co.cask.cdap.data2.registry.UsageDataset;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Has handy methods for dealing with Datasets.
 * todo: once we have couple methods, refactor out from "util" into smth more sensible
 */
public final class DatasetsUtil {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetsUtil.class);

  private DatasetsUtil() {}

  /**
   * Gets instance of {@link Dataset}, while add instance to
   * {@link co.cask.cdap.data2.dataset2.DatasetFramework} and creating the physical data set
   * if that one doesn't exist.
   * NOTE: does poor job guarding against races, i.e. only one client for this dataset instance is supported at a time
   */
  public static <T extends Dataset> T getOrCreateDataset(DatasetFramework datasetFramework,
                                                         DatasetId datasetInstanceId, String typeName,
                                                         DatasetProperties props,
                                                         @Nullable Map<String, String> arguments)
    throws DatasetManagementException, IOException {
    createIfNotExists(datasetFramework, datasetInstanceId, typeName, props);
    if (arguments == null) {
      arguments = Collections.emptyMap();
    }
    return datasetFramework.getDataset(datasetInstanceId, arguments, null);
  }

  /**
   * Gets a {@link Dataset} instance through the given {@link DatasetContext}. If the dataset doesn't exist,
   * it will be created using the given {@link DatasetFramework}.
   */
  public static <T extends Dataset> T getOrCreateDataset(DatasetContext datasetContext,
                                                         DatasetFramework datasetFramework,
                                                         DatasetId datasetId,
                                                         String datasetTypename,
                                                         DatasetProperties datasetProperties)
    throws DatasetManagementException, IOException {

    try {
      return datasetContext.getDataset(datasetId.getNamespace(), datasetId.getDataset());
    } catch (DatasetInstantiationException e) {
      createIfNotExists(datasetFramework, datasetId, datasetTypename, datasetProperties);
      return datasetContext.getDataset(datasetId.getNamespace(), datasetId.getDataset());
    }
  }

  /**
   * Creates instance of the data set if not exists
   */
  public static void createIfNotExists(DatasetFramework datasetFramework,
                                       DatasetId datasetInstanceId, String typeName,
                                       DatasetProperties props) throws DatasetManagementException, IOException {

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
   * For a dataset spec that does not contain the original properties, we attempt to reconstruct them from
   * the properties at the top-level of the spec. For most datasets, these will be identical, however, there
   * are a few known dataset types whose {@link DatasetDefinition#configure(String, DatasetProperties)} method
   * adds additional properties. As of release 3.3, the set of built-in such dataset types is known. Any dataset
   * created or reconfigured beginning with 3.4 will have the original properties stored in the spec.
   * @param spec a dataset spec that does not contain the original properties
   * @return the input spec if it is null or if it has original properties; otherwise a spec that has the
   *         original properties with which the dataset was created or reconfigured, at best effort.
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
      } else if (TimePartitionedFileSet.class.getName().equals(type) || "timePartitionedFileSet".equals(type)) {
        props.remove(PartitionedFileSetProperties.PARTITIONING_FIELDS);
        for (String key : spec.getProperties().keySet()) {
          if (key.startsWith(PartitionedFileSetProperties.PARTITIONING_FIELD_PREFIX)) {
            props.remove(key);
          }
        }

        // ObjectMappedTable adds the table schema and its row field name
      } else if (ObjectMappedTable.class.getName().endsWith(type) || "objectMappedTable".equals(type)) {
        props.remove(Table.PROPERTY_SCHEMA);
        props.remove(Table.PROPERTY_SCHEMA_ROW_FIELD);

        // LineageDataset and UsageDataset add the conflict level of none
      } else if (UsageDataset.class.getSimpleName().equals(type) ||
        LineageDataset.class.getName().equals(type) || "lineageDataset".equals(type)) {
        props.remove(Table.PROPERTY_CONFLICT_LEVEL);
      }
    }
    return spec.setOriginalProperties(props);
  }

  //TODO: CDAP-4627 - Figure out a better way to identify system datasets in user namespaces
  public static boolean isUserDataset(DatasetId datasetInstanceId) {
    return !NamespaceId.SYSTEM.equals(datasetInstanceId.getParent()) &&
      !isSystemDatasetInUserNamespace(datasetInstanceId);
  }

  public static boolean isSystemDatasetInUserNamespace(DatasetId datasetInstanceId) {
    return !NamespaceId.SYSTEM.equals(datasetInstanceId.getParent()) &&
      ("system.queue.config".equals(datasetInstanceId.getEntityName()) ||
      datasetInstanceId.getEntityName().startsWith("system.sharded.queue") ||
      datasetInstanceId.getEntityName().startsWith("system.queue") ||
      datasetInstanceId.getEntityName().startsWith("system.stream"));
  }

  /**
   * Returns whether or not the dataset defined in the given specification is transactional.
   * Defaults to true. Note that this should be in TableProperties, but because we do not expose
   * this setting through the API currently, keeping it here for now. See
   * https://issues.cask.co/browse/CDAP-1193 for additional information.
   */
  public static boolean isTransactional(Map<String, String> props) {
    return !"true".equalsIgnoreCase(props.get(Constants.Dataset.TABLE_TX_DISABLED));
  }
}
