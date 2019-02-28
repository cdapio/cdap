/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
package co.cask.cdap.data.runtime;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import com.google.inject.Module;

/**
 * DataFabricModules defines all of the bindings for the different data
 * fabric modes.
 */
public class DataFabricModules extends RuntimeModule {
  private final String txClientId;
  private final boolean useNoopTxClient;

  public DataFabricModules(String txClientId, CConfiguration cConf) {
    this(txClientId,
         cConf.get(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION).equals(Constants.Dataset.DATA_STORAGE_SQL));
  }

  public DataFabricModules() {
    this("", false);
  }

  public DataFabricModules(String txClientId, boolean useNoopTxClient) {
    this.txClientId = txClientId;
    this.useNoopTxClient = useNoopTxClient;
  }

  @Override
  public Module getInMemoryModules() {
    return new DataFabricInMemoryModule(txClientId, useNoopTxClient);
  }

  @Override
  public Module getStandaloneModules() {
    return new DataFabricLevelDBModule(useNoopTxClient);
  }

  @Override
  public Module getDistributedModules() {
    return new DataFabricDistributedModule(txClientId, useNoopTxClient);
  }

} // end of DataFabricModules
