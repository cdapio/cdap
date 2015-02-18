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
package co.cask.cdap.authorization;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.ACLStoreTable;
import co.cask.cdap.proto.Id;
import co.cask.common.authorization.client.ACLStoreSupplier;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Provides {@link ACLStoreTable}.
 */
@Singleton
public class ACLStoreTableSupplier implements ACLStoreSupplier {

  private static final String TABLE_NAME = "aclStoreTable";
  private static final Id.DatasetInstance TABLE_ID = new Id.DatasetInstance(
    new Id.Namespace(Constants.SYSTEM_NAMESPACE), TABLE_NAME);

  private final Supplier<ACLStoreTable> supplier;

  @Inject
  public ACLStoreTableSupplier(final DatasetFramework datasetFramework) {
    this.supplier = Suppliers.memoize(new Supplier<ACLStoreTable>() {

      @Override
      public ACLStoreTable get() {
        try {
          return DatasetsUtil.getOrCreateDataset(datasetFramework, TABLE_ID, ACLStoreTable.class.getName(),
                                                 DatasetProperties.EMPTY, ImmutableMap.<String, String>of(), null);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }

  @Override
  public ACLStoreTable get() {
    return supplier.get();
  }
}
