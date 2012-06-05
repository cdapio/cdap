package com.continuuity.gateway.runtime;

import com.continuuity.gateway.Consumer;
import com.continuuity.gateway.consumer.TransactionalConsumer;
import com.google.inject.AbstractModule;

/**
 * GatewayProductionModule defines the production bindings for the Gateway.
 */
public class GatewayProductionModule extends AbstractModule {

    public void configure() {

      // Bind our implementations
      bind(Consumer.class).to(TransactionalConsumer.class);

    }

} // end of GatewayProductionModule
