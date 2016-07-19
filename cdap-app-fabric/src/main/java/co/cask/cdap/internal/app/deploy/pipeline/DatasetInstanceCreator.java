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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Creates dataset instances.
 */
final class DatasetInstanceCreator {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceCreator.class);
  private final DatasetFramework datasetFramework;
  private final boolean allowDatasetUncheckedUpgrade;

  DatasetInstanceCreator(CConfiguration configuration, DatasetFramework datasetFramework) {
    this.datasetFramework = datasetFramework;
    this.allowDatasetUncheckedUpgrade = configuration.getBoolean(Constants.Dataset.DATASET_UNCHECKED_UPGRADE);
  }

  /**
   * Receives an input containing application specification and location
   * and verifies both.
   *
   * @param namespaceId the namespace to create the dataset instance in
   * @param datasets the datasets to create
   */
  void createInstances(NamespaceId namespaceId, Map<String, DatasetCreationSpec> datasets) throws Exception {
    // create dataset instances
    for (Map.Entry<String, DatasetCreationSpec> instanceEntry : datasets.entrySet()) {
      String instanceName = instanceEntry.getKey();
      DatasetId instanceId = namespaceId.dataset(instanceName);
      DatasetCreationSpec instanceSpec = instanceEntry.getValue();
      DatasetSpecification existingSpec = datasetFramework.getDatasetSpec(instanceId.toId());
      if (existingSpec == null) {
        LOG.info("Adding dataset instance: {}", instanceName);
        datasetFramework.addInstance(instanceSpec.getTypeName(), instanceId.toId(), instanceSpec.getProperties());
      } else {
        if (!existingSpec.getType().equals(instanceSpec.getTypeName())) {
          throw new IncompatibleUpdateException(
            String.format("Existing dataset '%s' of type '%s' may not be updated to type '%s'",
                          instanceName, existingSpec.getType(), instanceSpec.getTypeName()));
        }
        if (allowDatasetUncheckedUpgrade) {
          LOG.info("Updating dataset instance: {}", instanceName);
          datasetFramework.updateInstance(instanceId.toId(), instanceSpec.getProperties());
        }
      }
    }
  }
}
