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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import com.continuuity.tephra.TransactionSystemClient;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

/**
 * Builds {@link DefaultStore}
 */
public class DefaultStoreFactory implements StoreFactory {
  private final CConfiguration configuration;
  private final LocationFactory lFactory;
  private final TransactionSystemClient txClient;
  private final DatasetFramework dsFramework;

  @Inject
  public DefaultStoreFactory(CConfiguration configuration,
                             TransactionSystemClient txClient,
                             LocationFactory lFactory,
                             DatasetFramework dsFramework) {
    this.configuration = configuration;
    this.lFactory = lFactory;
    this.txClient = txClient;
    this.dsFramework = dsFramework;
  }

  @Override
  public Store create() {
    return new DefaultStore(configuration, lFactory, txClient, dsFramework);
  }
}
