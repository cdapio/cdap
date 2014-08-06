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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;

/**
 * Common utility for managing system metadata tables needed by various services.
 */
public abstract class MetaTableUtil {

  protected final DatasetFramework dsFramework;

  public MetaTableUtil(DatasetFramework framework, CConfiguration conf) {
    this.dsFramework =
      new NamespacedDatasetFramework(framework, new DefaultDatasetNamespace(conf, Namespace.SYSTEM));
  }

  public OrderedTable getMetaTable() throws Exception {
    return DatasetsUtil.getOrCreateDataset(dsFramework, getMetaTableName(), OrderedTable.class.getName(),
                                           DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
  }

  public void upgrade() throws Exception {
    DatasetAdmin admin = dsFramework.getAdmin(getMetaTableName(), null);
    if (admin != null) {
      admin.upgrade();
    }
  }

  /**
   * Returns the name of the metadata table to be used.
   */
  public abstract String getMetaTableName();
}
