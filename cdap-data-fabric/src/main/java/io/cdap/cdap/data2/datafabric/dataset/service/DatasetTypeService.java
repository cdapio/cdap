/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.dataset.module.DatasetType;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.util.List;

/**
 * Manages lifecycle of dataset {@link DatasetType types} and {@link DatasetModule modules}.
 */
public interface DatasetTypeService extends Service {

  /**
   * Returns all {@link DatasetModuleMeta dataset modules} in the specified {@link NamespaceId
   * namespace}.
   *
   * @param namespaceId the namespace to list dataset modules for
   * @return the list of dataset modules in the namespace
   */
  List<DatasetModuleMeta> listModules(NamespaceId namespaceId) throws Exception;

  /**
   * Returns the {@link DatasetModuleMeta metadata} of the specified {@link DatasetModuleId}.
   *
   * @param datasetModuleId the dataset module id to get the module meta
   * @return the dataset module meta for the id
   */
  DatasetModuleMeta getModule(DatasetModuleId datasetModuleId) throws Exception;

  /**
   * Deletes the specified {@link DatasetModuleId}
   */
  void delete(DatasetModuleId datasetModuleId) throws Exception;

  /**
   * Deletes all {@link DatasetModuleMeta dataset modules} in the specified {@link NamespaceId
   * namespace}.
   */
  void deleteAll(NamespaceId namespaceId) throws Exception;

  /**
   * Lists all {@link DatasetType dataset types} in the specified {@link NamespaceId}.
   */
  List<DatasetTypeMeta> listTypes(NamespaceId namespaceId) throws Exception;

  /**
   * Returns details of the specified {@link DatasetTypeId dataset type}.
   */
  DatasetTypeMeta getType(DatasetTypeId datasetTypeId) throws Exception;
}
