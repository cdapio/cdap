package com.continuuity.gateway.consumer;

import com.continuuity.api.flow.flowlet.Event;
import com.continuuity.gateway.Consumer;

import java.util.List;

/**
 * NoopConsumer is a consumer that does nothing. It can be used to
 * benchmark the performance overhead of the gateway.
 */
public class NoopConsumer extends Consumer {

  @Override
  protected void single(Event event) {
    // do nothing on purpose
  }

  @Override
  protected void batch(List<Event> events) {
    // do nothing on purpose
  }
}