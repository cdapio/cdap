/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.InstanceConflictException;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.proto.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Creates dataset instances.
 */
public class DatasetInstanceCreator {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceCreator.class);
  private final Id.Namespace namespace;
  private final DatasetFramework datasetFramework;
  private final boolean allowDatasetUncheckedUpgrade;

  public DatasetInstanceCreator(CConfiguration configuration, DatasetFramework datasetFramework,
                                Id.Namespace namespace) {
    this.namespace = namespace;
    this.datasetFramework = datasetFramework;
    this.allowDatasetUncheckedUpgrade = configuration.getBoolean(Constants.Dataset.DATASET_UNCHECKED_UPGRADE);
  }

  /**
   * Receives an input containing application specification and location
   * and verifies both.
   *
   * @param datasets the datasets to create
   */
  public void createInstances(Map<String, DatasetCreationSpec> datasets) throws Exception {
    // create dataset instances
    for (Map.Entry<String, DatasetCreationSpec> instanceEntry : datasets.entrySet()) {
      String instanceName = instanceEntry.getKey();
      Id.DatasetInstance instanceId = Id.DatasetInstance.from(namespace, instanceName);
      DatasetCreationSpec instanceSpec = instanceEntry.getValue();
      try {
        if (!datasetFramework.hasInstance(instanceId) || allowDatasetUncheckedUpgrade) {
          LOG.info("Adding instance: {}", instanceName);
          datasetFramework.addInstance(instanceSpec.getTypeName(), instanceId, instanceSpec.getProperties());
        }
      } catch (InstanceConflictException e) {
        // NO-OP: Instance is simply already created, possibly by an older version of this app OR a different app
        // TODO: verify that the created instance is from this app
        LOG.warn("Couldn't create dataset instance '" + instanceName + "' of type '" + instanceSpec.getTypeName(), e);
      }
    }
  }
}
