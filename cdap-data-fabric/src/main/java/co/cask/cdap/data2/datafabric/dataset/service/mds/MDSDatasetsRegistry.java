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

package co.cask.cdap.data2.datafabric.dataset.service.mds;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.tx.TransactionalDatasetRegistry;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;
import java.util.Map;

/**
 * Allows transactional operations with datasets metadata.
 */
public class MDSDatasetsRegistry extends TransactionalDatasetRegistry<MDSDatasets> {
  private final DatasetFramework dsFramework;

  private DatasetMetaTableUtil util;

  @Inject
  public MDSDatasetsRegistry(TransactionSystemClient txClient,
                             @Named("datasetMDS") DatasetFramework framework) {
    super(txClient);
    this.dsFramework = framework;
  }

  @Override
  public void startUp() throws Exception {
    this.util = new DatasetMetaTableUtil(dsFramework);
    this.util.init();
  }

  @Override
  public void shutDown() throws Exception {
  }

  @Override
  protected MDSDatasets createContext() throws IOException, DatasetManagementException {
    Map<String, ? extends Dataset> datasets = ImmutableMap.of(
      // "null" for class being in system classpath, for mds it is always true
      DatasetMetaTableUtil.INSTANCE_TABLE_NAME, util.getInstanceMetaTable(),
      DatasetMetaTableUtil.META_TABLE_NAME, util.getTypeMetaTable()
    );

    return new MDSDatasets(datasets);
  }
}
