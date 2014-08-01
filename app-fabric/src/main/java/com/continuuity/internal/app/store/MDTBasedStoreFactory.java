/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.store;

import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metadata.MetaDataTable;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

/**
 *
 */
public class MDTBasedStoreFactory implements StoreFactory {
  private final MetaDataTable table;
  private final CConfiguration configuration;
  private final LocationFactory lFactory;

  @Inject
  public MDTBasedStoreFactory(CConfiguration configuration,
                              MetaDataTable table,
                              LocationFactory lFactory) {
    this.configuration = configuration;
    this.table = table;
    this.lFactory = lFactory;
  }

  @Override
  public Store create() {
    return new MDTBasedStore(configuration, table, lFactory);
  }
}
