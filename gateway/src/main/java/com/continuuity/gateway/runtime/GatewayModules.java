package com.continuuity.gateway.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.gateway.Consumer;
import com.continuuity.gateway.consumer.NoopConsumer;
import com.continuuity.gateway.consumer.StreamEventWritingConsumer;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;

/**
 * GatewayModules defines the module bindings for the Gateway.
 */
public class GatewayModules extends RuntimeModule {

  private final com.continuuity.gateway.v2.runtime.GatewayModules gatewayV2Modules;

  public GatewayModules(CConfiguration cConfig) {
    this.gatewayV2Modules = new com.continuuity.gateway.v2.runtime.GatewayModules(cConfig);
  }

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
    return Modules.combine(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(Consumer.class).to(StreamEventWritingConsumer.class);
        }
      },
      gatewayV2Modules.getInMemoryModules()
    );
  }

  @Override
  public Module getSingleNodeModules() {
    return Modules.combine(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(Consumer.class).to(StreamEventWritingConsumer.class);
        }
      },
      gatewayV2Modules.getSingleNodeModules()
    );
  }

  @Override
  public Module getDistributedModules() {
    return Modules.combine(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(Consumer.class).to(StreamEventWritingConsumer.class);
        }
      },
      gatewayV2Modules.getDistributedModules()
    );
  }


} // end of GatewayModules
