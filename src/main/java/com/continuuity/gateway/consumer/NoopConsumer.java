package com.continuuity.gateway.consumer;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.gateway.Consumer;

/**
 * NoopConsumer is a consumer that does nothing. It can be used to
 * benchmark the performance overhead of the gateway.
 */
public class NoopConsumer extends Consumer {

  @Override
  protected void single(StreamEvent event, String accountId) {
    // do nothing on purpose
  }
}
