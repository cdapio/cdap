/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service.executor;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data2.datafabric.dataset.DatasetType;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.datafabric.dataset.type.DirectoryClassLoaderProvider;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.system.DatasetSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.SystemMetadataWriter;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handles Dataset admin operations.
 */
public class DatasetAdminService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetAdminService.class);

  private final RemoteDatasetFramework dsFramework;
  private final CConfiguration cConf;
  private final LocationFactory locationFactory;
  private final SystemDatasetInstantiatorFactory datasetInstantiatorFactory;
  private final MetadataStore metadataStore;

  @Inject
  public DatasetAdminService(RemoteDatasetFramework dsFramework, CConfiguration cConf, LocationFactory locationFactory,
                             SystemDatasetInstantiatorFactory datasetInstantiatorFactory, MetadataStore metadataStore) {
    this.dsFramework = dsFramework;
    this.cConf = cConf;
    this.locationFactory = locationFactory;
    this.datasetInstantiatorFactory = datasetInstantiatorFactory;
    this.metadataStore = metadataStore;
  }

  public boolean exists(Id.DatasetInstance datasetInstanceId) throws Exception {
    DatasetAdmin datasetAdmin = getDatasetAdmin(datasetInstanceId);
    return datasetAdmin.exists();
  }

  /**
   * Configures and creates a Dataset
   *
   * @param datasetInstanceId dataset instance to be created
   * @param typeMeta type meta for the dataset
   * @param props dataset instance properties
   * @param existing true, if dataset already exists (in case of update)
   * @return dataset specification
   * @throws Exception
   */
  public DatasetSpecification create(Id.DatasetInstance datasetInstanceId, DatasetTypeMeta typeMeta,
                                     DatasetProperties props, boolean existing) throws Exception {
    LOG.info("Creating dataset instance {}, type meta: {}, props: {}", datasetInstanceId, typeMeta, props);
    try (DatasetClassLoaderProvider classLoaderProvider =
           new DirectoryClassLoaderProvider(cConf, locationFactory)) {
      DatasetType type = dsFramework.getDatasetType(typeMeta, null, classLoaderProvider);

      if (type == null) {
        String msg = String.format("Cannot instantiate dataset type using provided type meta: %s", typeMeta);
        LOG.error(msg);
        throw new BadRequestException(msg);
      }

      DatasetSpecification spec = type.configure(datasetInstanceId.getId(), props);
      DatasetContext context = DatasetContext.from(datasetInstanceId.getNamespaceId());
      DatasetAdmin admin = type.getAdmin(context, spec);
      try {
        admin.create();

        writeSystemMetadata(datasetInstanceId, spec, props, typeMeta, type, context, existing);

        return spec;
      } catch (IOException e) {
        String msg = String.format("Error creating dataset \"%s\": %s", datasetInstanceId, e.getMessage());
        LOG.error(msg, e);
        throw new IOException(msg, e);
      }
    } catch (IOException e) {
      String msg = String.format("Error instantiating the dataset admin for dataset %s", datasetInstanceId);
      LOG.error(msg, e);
      throw new IOException(msg, e);
    }
  }

  private void writeSystemMetadata(Id.DatasetInstance datasetInstanceId, DatasetSpecification spec,
                                   DatasetProperties props, DatasetTypeMeta typeMeta, DatasetType type,
                                   DatasetContext context, boolean existing) throws IOException {
    // add system metadata for user datasets only
    if (isUserDataset(datasetInstanceId)) {
      Dataset dataset = null;
      try {
        try {
          dataset = type.getDataset(context, spec, DatasetDefinition.NO_ARGUMENTS);
        } catch (Exception e) {
          LOG.warn("Exception while instantiating Dataset {}", datasetInstanceId, e);
        }

        // Make sure to write whatever system metadata that can be derived
        // even if the above instantiation throws exception
        SystemMetadataWriter systemMetadataWriter;
        if (existing) {
          systemMetadataWriter =
            new DatasetSystemMetadataWriter(metadataStore, datasetInstanceId, props,
                                            dataset, typeMeta.getName(), spec.getDescription());
        } else {
          long createTime = System.currentTimeMillis();
          systemMetadataWriter =
            new DatasetSystemMetadataWriter(metadataStore, datasetInstanceId, props, createTime,
                                            dataset, typeMeta.getName(), spec.getDescription());
        }
        systemMetadataWriter.write();
      } finally {
        if (dataset != null) {
          dataset.close();
        }
      }
    }
  }

  public void drop(Id.DatasetInstance datasetInstanceId, DatasetTypeMeta typeMeta, DatasetSpecification spec)
    throws Exception {
    LOG.info("Dropping dataset with spec: {}, type meta: {}", spec, typeMeta);
    try (DatasetClassLoaderProvider classLoaderProvider =
           new DirectoryClassLoaderProvider(cConf, locationFactory)) {
      DatasetType type = dsFramework.getDatasetType(typeMeta, null, classLoaderProvider);

      if (type == null) {
        String msg = String.format("Cannot instantiate dataset type using provided type meta: %s", typeMeta);
        LOG.error(msg);
        throw new BadRequestException(msg);
      }

      DatasetAdmin admin = type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), spec);
      admin.drop();
    }

    // Remove metadata for the dataset
    metadataStore.removeMetadata(datasetInstanceId);
  }

  public void truncate(Id.DatasetInstance datasetInstanceId) throws Exception {
    DatasetAdmin datasetAdmin = getDatasetAdmin(datasetInstanceId);
    datasetAdmin.truncate();
  }

  public void upgrade(Id.DatasetInstance datasetInstanceId) throws Exception {
    DatasetAdmin datasetAdmin = getDatasetAdmin(datasetInstanceId);
    datasetAdmin.upgrade();
  }

  private DatasetAdmin getDatasetAdmin(Id.DatasetInstance datasetInstanceId) throws IOException,
    DatasetManagementException, NotFoundException {

    try (SystemDatasetInstantiator datasetInstantiator = datasetInstantiatorFactory.create()) {
      DatasetAdmin admin = datasetInstantiator.getDatasetAdmin(datasetInstanceId);
      if (admin == null) {
        throw new NotFoundException("Couldn't obtain DatasetAdmin for dataset instance " + datasetInstanceId);
      }
      return admin;
    }
  }

  //TODO: CDAP-4627 - Figure out a better way to identify system datasets in user namespaces
  private boolean isUserDataset(Id.DatasetInstance datasetInstanceId) {
    return !Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace()) &&
      !"system.queue.config".equals(datasetInstanceId.getId()) &&
      !datasetInstanceId.getId().startsWith("system.sharded.queue") &&
      !datasetInstanceId.getId().startsWith("system.queue") &&
      !datasetInstanceId.getId().startsWith("system.stream");
  }
}
