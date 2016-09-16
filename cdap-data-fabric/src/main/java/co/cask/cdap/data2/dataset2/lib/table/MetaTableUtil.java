/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;

import java.io.IOException;

/**
 * Common utility for managing system metadata tables needed by various services.
 */
public abstract class MetaTableUtil {

  protected final DatasetFramework dsFramework;

  public MetaTableUtil(DatasetFramework framework, CConfiguration conf) {
    this.dsFramework = framework;
  }

  public Table getMetaTable() throws IOException, DatasetManagementException {
    DatasetId metaTableInstanceId = NamespaceId.SYSTEM.dataset(getMetaTableName());
    return DatasetsUtil.getOrCreateDataset(dsFramework, metaTableInstanceId, Table.class.getName(),
                                           DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
  }

  /**
   * Returns the name of the metadata table to be used.
   */
  public abstract String getMetaTableName();
}
