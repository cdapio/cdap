package com.continuuity.runtime;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.metadata.MetadataServer;
import com.continuuity.metadata.MetadataServerInterface;
import com.google.inject.AbstractModule;
import com.google.inject.Module;

/**
 * Module for metadata server.
 */
public class MetadataModules extends RuntimeModule {
  /**
   * Implementers of this method should return a combined Module that includes
   * all of the modules and classes required to instantiate and run an in
   * memory instance of Continuuity.
   *
   * @return A combined set of Modules required for InMemory execution.
   */
  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetadataServerInterface.class).to(MetadataServer.class);
      }
    };
  }

  /**
   * Implementers of this method should return a combined Module that includes
   * all of the modules and classes required to instantiate and run an a single
   * node instance of Continuuity.
   *
   * @return A combined set of Modules required for SingleNode execution.
   */
  @Override
  public Module getSingleNodeModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetadataServerInterface.class).to(MetadataServer.class);
      }
    };
  }

  /**
   * Implementers of this method should return a combined Module that includes
   * all of the modules and classes required to instantiate and the fully
   * distributed Continuuity PaaS.
   *
   * @return A combined set of Modules required for distributed execution.
   */
  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetadataServerInterface.class).to(MetadataServer.class);
      }
    };
  }
}
