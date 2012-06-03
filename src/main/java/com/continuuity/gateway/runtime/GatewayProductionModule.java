package com.continuuity.gateway.runtime;

import com.continuuity.gateway.QueueWritingConsumer;
import com.google.inject.AbstractModule;

import com.continuuity.gateway.Consumer;

/**
 * GatewayProductionModule defines the production bindings for the Gateway.
 */
public class GatewayProductionModule extends AbstractModule {

    public void configure() {

      // Bind our implementations
      bind(Consumer.class).to(QueueWritingConsumer.class);

    }

} // end of GatewayProductionModule
