package com.continuuity.gateway.v2;

/**
 * Constants used in Gateway Configuration.
 */
public final class GatewayV2Constants {

  /**
   * Key names of Gateway Configuration.
   */
  public static final class ConfigKeys {
    public static final String ADDRESS = "gateway.v2.bind.address";
    public static final String PORT = "gateway.v2.port";
    public static final String BACKLOG = "gateway.v2.connection.backlog";
    public static final String EXEC_THREADS = "gateway.v2.exec.threads";
    public static final String MAX_CACHED_STREAM_EVENTS_NUM = "gateway.v2.max.cached.stream.events.num";
    public static final String MAX_CACHED_EVENTS_PER_STREAM_NUM = "gateway.v2.max.cached.events.per.stream.num";
    public static final String MAX_CACHED_STREAM_EVENTS_BYTES = "gateway.v2.max.cached.stream.events.bytes";
    public static final String STREAM_EVENTS_FLUSH_INTERVAL_MS = "gateway.v2.stream.events.flush.interval.ms";
  }

  public static final int DEFAULT_PORT = 11000;
  public static final int DEFAULT_BACKLOG = 20000;
  public static final int DEFAULT_EXEC_THREADS = 20;
  public static final int DEFAULT_MAX_CACHED_STREAM_EVENTS_NUM = 10000;
  public static final int DEFAULT_MAX_CACHED_EVENTS_PER_STREAM_NUM = 5000;
  public static final long DEFAULT_MAX_CACHED_STREAM_EVENTS_BYTES = 50 * 1024 * 1024;
  public static final long DEFAULT_STREAM_EVENTS_FLUSH_INTERVAL_MS = 150;

  public static final String GATEWAY_V2_HTTP_HANDLERS = "gateway.v2.http.handler";
}
