/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway;

import com.continuuity.gateway.util.StreamCache;
import com.continuuity.metadata.MetadataService;

/**
 * This is the base class for all collectors. A collector is a type of Connector
 * that receives events from external clients via RPC call and writes them to
 * event streams in the data fabric. A collector can receive events over any
 * protocol, as long as it can convert the events from that protocol into an
 * Event, or a batch of List&lt;Event>. Events are passed to the Consumer, which
 * writes them to the data fabric. The consumer is set during initialization,
 * more precisely after configure() but before start().
 */
public abstract class Collector extends Connector implements MetaDataServiceAware {

  /**
   * The consumer to pass all events to.
   */
  protected Consumer consumer;

  /**
   * Cache for Stream meta data lookups.
   */
  protected StreamCache streamCache;

  /**
   * Set the consumer for this collector. It may be shared with other collectors
   *
   * @param consumer The consumer to use.
   */
  public void setConsumer(Consumer consumer) {
    this.consumer = consumer;
  }

  /**
   * Get the consumer of this collector.
   *
   * @return the collector's consumer.
   */
  public Consumer getConsumer() {
    return this.consumer;
  }

  public StreamCache getStreamCache() {
    return this.streamCache;
  }

  /**
   * Set the meta data service for this collector.
   *
   * @param service the metadata servrice to use
   */
  @Override
  public void setMetadataService(MetadataService service) {
    super.setMetadataService(service);
    this.streamCache = new StreamCache(service);
  }

}
