package com.continuuity.gateway.runtime;

import com.continuuity.gateway.Consumer;
import com.continuuity.gateway.consumer.TupleWritingConsumer;
import com.google.inject.AbstractModule;

/**
 * GatewayProductionModule defines the production bindings for the Gateway.
 */
public class GatewayProductionModule extends AbstractModule {

    public void configure() {

      // Bind our implementations
      bind(Consumer.class).to(TupleWritingConsumer.class);

    }

} // end of GatewayProductionModule
