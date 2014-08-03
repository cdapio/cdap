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
package co.cask.cdap.common.runtime;

import com.google.inject.Module;

/**
 * Runtime Module defines all of the methods that all of our Guice modules must
 * implement. We expect all modules that are found in each component's "runtime"
 * package to extend this class.
 */
// TODO: remove this interface as it doesn't make sense anymore, and hurt by suggesting not always suitable API
public abstract class RuntimeModule {

  /**
   * Implementers of this method should return a combined Module that includes
   * all of the modules and classes required to instantiate and run an in
   * memory instance of Continuuity.
   *
   * @return A combined set of Modules required for InMemory execution.
   */
  public abstract Module getInMemoryModules();

  /**
   * Implementers of this method should return a combined Module that includes
   * all of the modules and classes required to instantiate and run an a single
   * node instance of Continuuity.
   *
   * @return A combined set of Modules required for SingleNode execution.
   */
  public abstract Module getSingleNodeModules();

  /**
   * Implementers of this method should return a combined Module that includes
   * all of the modules and classes required to instantiate and the fully
   * distributed Continuuity PaaS.
   *
   * @return A combined set of Modules required for distributed execution.
   */
  public abstract Module getDistributedModules();

}
