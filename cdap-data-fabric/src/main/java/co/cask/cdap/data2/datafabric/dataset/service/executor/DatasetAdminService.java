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
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.api.dataset.Updatable;
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
import co.cask.cdap.data2.security.ImpersonationUtils;
import co.cask.cdap.data2.security.Impersonator;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

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
  private final Impersonator impersonator;

  @Inject
  public DatasetAdminService(RemoteDatasetFramework dsFramework, CConfiguration cConf, LocationFactory locationFactory,
                             SystemDatasetInstantiatorFactory datasetInstantiatorFactory, MetadataStore metadataStore,
                             Impersonator impersonator) {
    this.dsFramework = dsFramework;
    this.cConf = cConf;
    this.locationFactory = locationFactory;
    this.datasetInstantiatorFactory = datasetInstantiatorFactory;
    this.metadataStore = metadataStore;
    this.impersonator = impersonator;
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
   * @param existing if dataset already exists (in case of update), the existing properties
   * @return dataset specification
   * @throws Exception
   */
  public DatasetSpecification createOrUpdate(final Id.DatasetInstance datasetInstanceId, final DatasetTypeMeta typeMeta,
                                             final DatasetProperties props,
                                             @Nullable final DatasetSpecification existing) throws Exception {

    if (existing == null) {
      LOG.info("Creating dataset instance {}, type meta: {}, props: {}", datasetInstanceId, typeMeta, props);
    } else {
      LOG.info("Updating dataset instance {}, type meta: {}, existing: {}, props: {}",
               datasetInstanceId, typeMeta, existing, props);
    }
    try (DatasetClassLoaderProvider classLoaderProvider =
           new DirectoryClassLoaderProvider(cConf, locationFactory)) {
      final DatasetContext context = DatasetContext.from(datasetInstanceId.getNamespaceId());
      UserGroupInformation ugi = impersonator.getUGI(datasetInstanceId.getNamespace().toEntityId());

      final DatasetType type = ImpersonationUtils.doAs(ugi, new Callable<DatasetType>() {
        @Override
        public DatasetType call() throws Exception {
          DatasetType type = dsFramework.getDatasetType(typeMeta, null, classLoaderProvider);
          if (type == null) {
            throw new BadRequestException(
              String.format("Cannot instantiate dataset type using provided type meta: %s", typeMeta));
          }
          return type;
        }
      });

      DatasetSpecification spec = ImpersonationUtils.doAs(ugi, new Callable<DatasetSpecification>() {
        @Override
        public DatasetSpecification call() throws Exception {
          DatasetSpecification spec = existing == null ? type.configure(datasetInstanceId.getId(), props)
            : type.reconfigure(datasetInstanceId.getId(), props, existing);

          DatasetAdmin admin = type.getAdmin(context, spec);
          if (existing != null) {
            if (admin instanceof Updatable) {
              ((Updatable) admin).update(existing);
            } else {
              admin.upgrade();
            }
          } else {
            admin.create();
          }
          return spec;
        }
      });

      // Writing system metadata should be done without impersonation since user may not have access to system tables.
      writeSystemMetadata(datasetInstanceId, spec, props, typeMeta, type, context, existing != null, ugi);
      return spec;
    } catch (Exception e) {
      if (e instanceof IncompatibleUpdateException) {
        // this is expected to happen if user provides bad update properties, so we log this as debug
        LOG.debug("Incompatible update for dataset '{}'", datasetInstanceId, e);
      } else {
        LOG.error("Error {} dataset '{}': {}",
                  existing == null ? "creating" : "updating", datasetInstanceId, e.getMessage(), e);
      }
      throw e;
    }
  }

  private void writeSystemMetadata(Id.DatasetInstance datasetInstanceId, final DatasetSpecification spec,
                                   DatasetProperties props, final DatasetTypeMeta typeMeta, final DatasetType type,
                                   final DatasetContext context, boolean existing, UserGroupInformation ugi)
    throws IOException {
    // add system metadata for user datasets only
    if (isUserDataset(datasetInstanceId)) {
      Dataset dataset = null;
      try {
        try {
          dataset = ImpersonationUtils.doAs(ugi, new Callable<Dataset>() {
            @Override
            public Dataset call() throws Exception {
              return type.getDataset(context, spec, DatasetDefinition.NO_ARGUMENTS);
            }
          });
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

  public void drop(final Id.DatasetInstance datasetInstanceId, final DatasetTypeMeta typeMeta,
                   final DatasetSpecification spec) throws Exception {
    LOG.info("Dropping dataset with spec: {}, type meta: {}", spec, typeMeta);
    try (DatasetClassLoaderProvider classLoaderProvider =
           new DirectoryClassLoaderProvider(cConf, locationFactory)) {

      impersonator.doAs(datasetInstanceId.getNamespace().toEntityId(), new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          DatasetType type = dsFramework.getDatasetType(typeMeta, null, classLoaderProvider);

          if (type == null) {
            throw new BadRequestException(
              String.format("Cannot instantiate dataset type using provided type meta: %s", typeMeta));
          }
          DatasetAdmin admin = type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), spec);
          admin.drop();
          return null;
        }
      });
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

  private DatasetAdmin getDatasetAdmin(final Id.DatasetInstance datasetInstanceId) throws IOException,
    DatasetManagementException, NotFoundException {

    try (SystemDatasetInstantiator datasetInstantiator = datasetInstantiatorFactory.create()) {
      try {
        return impersonator.doAs(datasetInstanceId.getNamespace().toEntityId(), new Callable<DatasetAdmin>() {
          @Override
          public DatasetAdmin call() throws Exception {
            DatasetAdmin admin = datasetInstantiator.getDatasetAdmin(datasetInstanceId);
            if (admin == null) {
              throw new NotFoundException("Couldn't obtain DatasetAdmin for dataset instance " + datasetInstanceId);
            }
            // returns a DatasetAdmin that executes operations as a particular user, for a particular namespace
            return new ImpersonatingDatasetAdmin(admin, impersonator, datasetInstanceId.getNamespace().toEntityId());
          }
        });
      } catch (Exception e) {
        Throwables.propagateIfInstanceOf(e, IOException.class);
        Throwables.propagateIfInstanceOf(e, DatasetManagementException.class);
        Throwables.propagateIfInstanceOf(e, NotFoundException.class);
        throw Throwables.propagate(e);
      }
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
