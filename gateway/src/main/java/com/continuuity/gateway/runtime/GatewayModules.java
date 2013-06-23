package com.continuuity.gateway.runtime;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.gateway.Consumer;
import com.continuuity.gateway.consumer.NoopConsumer;
import com.continuuity.gateway.consumer.StreamEventWritingConsumer;
import com.google.inject.AbstractModule;
import com.google.inject.Module;

/**
 * GatewayModules defines the module bindings for the Gateway.
 */
public class GatewayModules extends RuntimeModule {

  public Module getNoopModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(Consumer.class).to(NoopConsumer.class);
      }
    };
  }

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(Consumer.class).to(StreamEventWritingConsumer.class);
      }
    };
  }

  @Override
  public Module getSingleNodeModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(Consumer.class).to(StreamEventWritingConsumer.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(Consumer.class).to(StreamEventWritingConsumer.class);
      }
    };
  }


} // end of GatewayModules
