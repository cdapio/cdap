/**
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway.collector;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.Collector;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.Consumer;

/**
 * A flume collector implements Flume's Avro protocol to receive
 * events from a Flume Avro sink. There may be different
 * implementations of the actual listener (such as plain netty,
 * or Camel based), but the logic of consuming a (batch of)
 * Flume event(s) is implemented in the FlumeAdapter.
 */
public abstract class FlumeCollector extends Collector {

  /**
   * If no port is ever configured, we run here.
   */
  public static final int DEFAULT_PORT = 8765;

  /**
   * The actual port to run on, set by configure().
   */
  protected int port = DEFAULT_PORT;

  /**
   * The adapter that converts from and to flume Avro and calls the consumer.
   */
  protected FlumeAdapter flumeAdapter;

  @Override
  public void setConsumer(Consumer consumer) {
    super.setConsumer(consumer);
    // we consume through the adapter, if necessary create one
    if (this.flumeAdapter == null) {
      this.flumeAdapter = new FlumeAdapter(this);
    }
  }

  @Override
  public void configure(CConfiguration configuration) throws Exception {
    super.configure(configuration);
    // the only option we need is the port number
    this.port = configuration.getInt(Constants.buildConnectorPropertyName(
      this.getName(), Constants.CONFIG_PORT), DEFAULT_PORT);
  }

  /**
   * Return the port that this collector is listening on.
   *
   * @return the port number
   */
  public int getPort() {
    return this.port;
  }

  /**
   * Helper method for printing in the log.
   */
  public String toString() {
    return this.getClass().getName() + " at :" + this.getPort() + " (" +
      (this.consumer == null ? "no consumer set" :
        this.consumer.getClass().getName()) + ")";
  }
}
