package com.continuuity.gateway.v2;

/**
 * Constants used in Gateway Configuration.
 */
public final class GatewayConstants {

  /**
   * Key names of Gateway Configuration.
   */
  public static final class ConfigKeys {
    public static final String ADDRESS = "gateway.server.address";
    public static final String PORT = "stream.rest.port";
    public static final String BACKLOG = "gateway.connection.backlog";
    public static final String EXEC_THREADS = "gateway.exec.threads";
    public static final String BOSS_THREADS = "gateway.boss.threads";
    public static final String WORKER_THREADS = "gateway.worker.threads";
    public static final String MAX_CACHED_STREAM_EVENTS_NUM = "gateway.max.cached.stream.events.num";
    public static final String MAX_CACHED_EVENTS_PER_STREAM_NUM = "gateway.max.cached.events.per.stream.num";
    public static final String MAX_CACHED_STREAM_EVENTS_BYTES = "gateway.max.cached.stream.events.bytes";
    public static final String STREAM_EVENTS_FLUSH_INTERVAL_MS = "gateway.stream.events.flush.interval.ms";
    public static final String STREAM_EVENTS_CALLBACK_NUM_THREADS = "gateway.stream.callback.exec.num.threads";
  }

  public static final int DEFAULT_PORT = 10000;
  public static final int DEFAULT_BACKLOG = 20000;
  public static final int DEFAULT_EXEC_THREADS = 20;
  public static final int DEFAULT_BOSS_THREADS = 1;
  public static final int DEFAULT_WORKER_THREADS = 10;
  public static final int DEFAULT_MAX_CACHED_STREAM_EVENTS_NUM = 10000;
  public static final int DEFAULT_MAX_CACHED_EVENTS_PER_STREAM_NUM = 5000;
  public static final long DEFAULT_MAX_CACHED_STREAM_EVENTS_BYTES = 50 * 1024 * 1024;
  public static final long DEFAULT_STREAM_EVENTS_FLUSH_INTERVAL_MS = 150;
  public static final int DEFAULT_STREAM_EVENTS_CALLBACK_NUM_THREADS = 5;

  public static final String GATEWAY_V2_HTTP_HANDLERS = "gateway.http.handler";

  // Handler names
  public static final String STREAM_HANDLER_NAME = "stream.rest";
}
