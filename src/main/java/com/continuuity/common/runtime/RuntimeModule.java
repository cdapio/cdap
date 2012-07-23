/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.common.runtime;

import com.google.inject.Module;

/**
 * Runtime Module defines all of the methods that all of our Guice modules must
 * implement. We expect all modules that are found in each component's "runtime"
 * package to extend this class.
 */
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
