/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.data.runtime.DatasetClassLoaderUtil;
import co.cask.cdap.data.runtime.DatasetClassLoaders;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.proto.DatasetTypeMeta;
import com.google.common.base.Preconditions;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Utility class to get {@link DatasetAdminWrapper} {@link DatasetTypeWrapper} {@link DatasetWrapper}
 */
public class DatasetWrapperUtility {

  public static DatasetAdminWrapper getDatasetAdminWrapper(DatasetFramework dsFramework, String instanceName,
                                                           LocationFactory locationFactory,
                                                           ClassLoader parentClassLoader)
    throws IOException, DatasetManagementException {

    Preconditions.checkNotNull(dsFramework.getDatasetSpec(instanceName));
    DatasetTypeMeta typeMeta = dsFramework.getType(dsFramework.getDatasetSpec(instanceName).getType());
    Preconditions.checkNotNull(typeMeta);

    DatasetClassLoaderUtil dsUtil = DatasetClassLoaders.createDatasetClassLoaderFromType
      (parentClassLoader, typeMeta, locationFactory);
    DatasetAdmin admin = dsFramework.getAdmin(instanceName, dsUtil.getClassLoader());
    Preconditions.checkNotNull(admin);

    return new DatasetAdminWrapper(dsUtil, admin);
  }

  public static DatasetTypeWrapper getDatasetTypeWrapper(RemoteDatasetFramework dsFramework, DatasetTypeMeta typeMeta,
                                                   LocationFactory locationFactory,
                                                   ClassLoader parentClassLoader)
    throws IOException, DatasetManagementException {

    Preconditions.checkNotNull(typeMeta);
    DatasetClassLoaderUtil dsUtil = DatasetClassLoaders.createDatasetClassLoaderFromType
      (parentClassLoader, typeMeta, locationFactory);
    DatasetType datasetType = dsFramework.getDatasetType(typeMeta, dsUtil.getClassLoader());

    Preconditions.checkNotNull(datasetType);
    return new DatasetTypeWrapper(dsUtil, datasetType);
  }

  public static DatasetWrapper getDatasetWrapper(DatasetFramework datasetFramework, String instanceName,
                                                 Map<String, String> arguments,
                                                 LocationFactory locationFactory,
                                                 ClassLoader parentClassLoader)
    throws IOException, DatasetManagementException {


    Preconditions.checkNotNull(datasetFramework.getDatasetSpec(instanceName));

    DatasetTypeMeta typeMeta = datasetFramework.getType(datasetFramework.getDatasetSpec(instanceName).getType());
    Preconditions.checkNotNull(typeMeta);

    DatasetClassLoaderUtil dsUtil = DatasetClassLoaders.createDatasetClassLoaderFromType
      (parentClassLoader, typeMeta, locationFactory);

    Dataset dataset = datasetFramework.getDataset(instanceName, arguments, dsUtil.getClassLoader());
    Preconditions.checkNotNull(dataset);
    return new DatasetWrapper(dsUtil, dataset);
  }
}
