/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.Id;

/**
 * Common utility for managing system metadata tables needed by various services.
 */
public abstract class MetaTableUtil {

  // Namespace for meta tables is 'system'
  protected static final Id.Namespace SYSTEM_NAMESPACE = Id.Namespace.from(Constants.SYSTEM_NAMESPACE);

  protected final DatasetFramework dsFramework;

  public MetaTableUtil(DatasetFramework framework, CConfiguration conf) {
    this.dsFramework = framework;
  }

  public Table getMetaTable() throws Exception {
    Id.DatasetInstance metaTableInstanceId = Id.DatasetInstance.from(SYSTEM_NAMESPACE, getMetaTableName());
    return DatasetsUtil.getOrCreateDataset(dsFramework, metaTableInstanceId, Table.class.getName(),
                                           DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
  }

  public void upgrade() throws Exception {
    DatasetAdmin admin = dsFramework.getAdmin(Id.DatasetInstance.from(SYSTEM_NAMESPACE, getMetaTableName()), null);
    if (admin != null) {
      admin.upgrade();
    }
  }

  /**
   * Returns the name of the metadata table to be used.
   */
  public abstract String getMetaTableName();
}
