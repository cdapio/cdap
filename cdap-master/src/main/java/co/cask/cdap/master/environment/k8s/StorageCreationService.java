/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.master.environment.k8s;

import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;

/**
 * A Guava {@link Service} for creating storages.
 */
final class StorageCreationService extends AbstractIdleService {

  private final StructuredTableAdmin tableAdmin;
  private final StructuredTableRegistry tableRegistry;
  private final MetadataStore metadataStore;

  @Inject
  StorageCreationService(StructuredTableAdmin tableAdmin,
                         StructuredTableRegistry tableRegistry, MetadataStore metadataStore) {
    this.tableAdmin = tableAdmin;
    this.tableRegistry = tableRegistry;
    this.metadataStore = metadataStore;
  }

  @Override
  protected void startUp() throws Exception {
    StoreDefinition.createAllTables(tableAdmin, tableRegistry);
    metadataStore.createIndex();
  }

  @Override
  protected void shutDown() {
    // no-op
  }
}
