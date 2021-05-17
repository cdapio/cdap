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

package io.cdap.cdap.data2.dataset2.lib.table;

import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data2.datafabric.dataset.DatasetsUtil;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;

/**
 * Common utility for managing system metadata tables needed by various services.
 */
public abstract class MetaTableUtil {

  protected final DatasetFramework dsFramework;

  public MetaTableUtil(DatasetFramework framework, CConfiguration conf) {
    this.dsFramework = framework;
  }

  public Table getMetaTable() throws IOException, DatasetManagementException, UnauthorizedException {
    DatasetId metaTableInstanceId = NamespaceId.SYSTEM.dataset(getMetaTableName());
    return DatasetsUtil.getOrCreateDataset(dsFramework, metaTableInstanceId, Table.class.getName(),
                                           DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS);
  }

  /**
   * Returns the name of the metadata table to be used.
   */
  public abstract String getMetaTableName();
}
