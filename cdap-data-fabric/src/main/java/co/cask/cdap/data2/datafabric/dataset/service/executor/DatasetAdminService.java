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

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
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

  @Inject
  public DatasetAdminService(RemoteDatasetFramework dsFramework, CConfiguration cConf, LocationFactory locationFactory,
                             SystemDatasetInstantiatorFactory datasetInstantiatorFactory) {
    this.dsFramework = dsFramework;
    this.cConf = cConf;
    this.locationFactory = locationFactory;
    this.datasetInstantiatorFactory = datasetInstantiatorFactory;
  }

  public boolean exists(Id.DatasetInstance datasetInstanceId) throws Exception {
    DatasetAdmin datasetAdmin = getDatasetAdmin(datasetInstanceId);
    return datasetAdmin.exists();
  }

  public DatasetSpecification create(Id.DatasetInstance datasetInstanceId, DatasetTypeMeta typeMeta,
                                     DatasetProperties props) throws Exception {
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
      DatasetAdmin admin = type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), spec);
      try {
        admin.create();
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
}
