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
package co.cask.cdap.data.runtime;

import co.cask.cdap.common.runtime.RuntimeModule;
import com.google.inject.Module;

/**
 * DataFabricModules defines all of the bindings for the different data
 * fabric modes.
 */
public class DataFabricModules extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new DataFabricInMemoryModule();
  }

  @Override
  public Module getSingleNodeModules() {
    return new DataFabricLocalModule();
  }

  @Override
  public Module getDistributedModules() {
    return new DataFabricDistributedModule();
  }

} // end of DataFabricModules
