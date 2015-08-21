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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.InstanceConflictException;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

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
                                                         Id.DatasetInstance datasetInstanceId, String typeName,
                                                         DatasetProperties props,
                                                         Map<String, String> arguments,
                                                         ClassLoader cl)
    throws DatasetManagementException, IOException {

    createIfNotExists(datasetFramework, datasetInstanceId, typeName, props);
    return (T) datasetFramework.getDataset(datasetInstanceId, arguments, null);
  }

  /**
   * Creates instance of the data set if not exists
   */
  public static void createIfNotExists(DatasetFramework datasetFramework,
                                       Id.DatasetInstance datasetInstanceId, String typeName,
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
}
