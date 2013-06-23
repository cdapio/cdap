package com.continuuity.gateway.consumer;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.gateway.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Println Consumer is a very simple consumer that INFO logs all events in the
 * form [ Headers: $Headers | Body: $Body].
 */
public class PrintlnConsumer extends Consumer {

  /**
   * This is our Logger instance.
   */
  private static final Logger LOG =
    LoggerFactory.getLogger(PrintlnConsumer.class);

  /**
   * Very basic implementation that logs out the headers and body.
   *
   * @param event the event to be consumed
   * @throws Exception
   */
  @Override
  protected void single(StreamEvent event, String accountId) throws Exception {
    LOG.info("[ Account: " + accountId + " | Headers: " + event.getHeaders() + " | "
               + "Body: " + event.getBody());
  }
}
