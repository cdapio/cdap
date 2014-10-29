/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.InstanceConflictException;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.pipeline.Stage;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link Stage} is responsible for creating PropertyTable dataset for the application.
 */
public class CreatePropertyDatasetStage extends AbstractStage<ApplicationSpecLocation> {
  private static final Logger LOG = LoggerFactory.getLogger(CreatePropertyDatasetStage.class);
  private final DatasetFramework datasetFramework;

  public CreatePropertyDatasetStage(DatasetFramework datasetFramework) {
    super(TypeToken.of(ApplicationSpecLocation.class));
    this.datasetFramework = datasetFramework;
  }

  /**
   * Gets the accountId, applicationId to formulate the dataset name.
   *
   * @param input An instance of {@link ApplicationSpecLocation}
   */
  @Override
  public void process(ApplicationSpecLocation input) throws Exception {
    String propertyTableName = Joiner.on(".").join(Lists.newArrayList(input.getApplicationId().getAccountId(),
                                                                      input.getApplicationId().getId(),
                                                                      Constants.Dataset.PROPERTY_TABLE));
    try {
      // create the propertyTable dataset if it doesn't exist already
      if (!datasetFramework.hasInstance(propertyTableName)) {
        datasetFramework.addInstance(KeyValueTable.class.getName(), propertyTableName, DatasetProperties.EMPTY);
      }
    } catch (InstanceConflictException e) {
      LOG.warn("Couldn't create property dataset instance", e);
    }

    emit(input);
  }
}
