package com.continuuity.gateway.runtime;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.gateway.Consumer;
import com.continuuity.gateway.consumer.TupleWritingConsumer;
import com.google.inject.AbstractModule;
import com.google.inject.Module;

/**
 * GatewayModules defines the module bindings for the Gateway.
 */
public class GatewayModules extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(Consumer.class).to(TupleWritingConsumer.class);
      }
    };
  }

  @Override
  public Module getSingleNodeModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(Consumer.class).to(TupleWritingConsumer.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    throw new UnsupportedOperationException(
        "Distributed mode is not yet implemented");
  }


} // end of GatewayModules
