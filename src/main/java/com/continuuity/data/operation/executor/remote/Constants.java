package com.continuuity.data.operation.executor.remote;

/**
 * Provides constants for cnfiguration of the opex service and the remote opex.
 */
public class Constants {
  // Configuration key names and defaults used by opex service.

  /** for the port of the opex server. */
  public static final String CFG_DATA_OPEX_SERVER_PORT
      = "data.opex.server.port";

  /** for the address (hostname) of the opex server. */
  public static final String CFG_DATA_OPEX_SERVER_ADDRESS
      = "data.opex.server.address";

  /** the number of handler threads in the opex service. */
  public static final String CFG_DATA_OPEX_SERVER_THREADS
      = "data.opex.server.threads";

  /** default opex service port. */
  public static final int    DEFAULT_DATA_OPEX_SERVER_PORT
      = 15165;

  /** default opex service address. */
  public static final String DEFAULT_DATA_OPEX_SERVER_ADDRESS
      = "0.0.0.0";

  /** default number of handler threads in opex service. */
  public static final int    DEFAULT_DATA_OPEX_SERVER_THREADS
      = 20;

  // Configuration key names and defaults used by opex client.

  /** to specify the opex client socket timeout in ms. */
  public static final String CFG_DATA_OPEX_CLIENT_TIMEOUT
      = "data.opex.client.timeout";

  /** to specify the opex client socket timeout for long-running ops in ms. */
  public static final String CFG_DATA_OPEX_CLIENT_LONG_TIMEOUT
      = "data.opex.client.long.timeout";

  /** to specify the opex client provider strategy. */
  public static final String CFG_DATA_OPEX_CLIENT_PROVIDER
      = "data.opex.client.provider";

  /** to specify the number of threads for client provider "pool". */
  public static final String CFG_DATA_OPEX_CLIENT_COUNT
      = "data.opex.client.count";

  /** to specify the retry strategy for a failed thrift call. */
  public static final String CFG_DATA_OPEX_CLIENT_RETRY_STRATEGY
      = "data.opex.client.retry.strategy";

  /** to specify the number of times to retry a failed thrift call. */
  public static final String CFG_DATA_OPEX_CLIENT_ATTEMPTS
      = "data.opex.client.retry.attempts";

  /** to specify the initial sleep time for retry strategy backoff. */
  public static final String CFG_DATA_OPEX_CLIENT_BACKOFF_INIITIAL
      = "data.opex.client.retry.backoff.initial";

  /** to specify the backoff factor for retry strategy backoff. */
  public static final String CFG_DATA_OPEX_CLIENT_BACKOFF_FACTOR
      = "data.opex.client.retry.backoff.factor";

  /** to specify the sleep time limit for retry strategy backoff. */
  public static final String CFG_DATA_OPEX_CLIENT_BACKOFF_LIMIT
      = "data.opex.client.retry.backoff.limit";

  /** the default opex client socket timeout in milli seconds. */
  public static final int DEFAULT_DATA_OPEX_CLIENT_TIMEOUT
      = 30 * 1000;

  /** opex client timeout for long operations such as ClearFabric. */
  public static final int DEFAULT_DATA_OPEX_CLIENT_LONG_TIMEOUT
      = 300 * 1000;

  /** default number of pooled opex clients. */
  public static final int    DEFAULT_DATA_OPEX_CLIENT_COUNT
      = 5;

  /** default opex client provider strategy. */
  public static final String DEFAULT_DATA_OPEX_CLIENT_PROVIDER
      = "pool";

  /** retry strategy for thrift clients, e.g. backoff, or n-times. */
  public static final String DEFAULT_DATA_OPEX_CLIENT_RETRY_STRATEGY
      = "backoff";

  /** default number of attempts for strategy n-times. */
  public static final int DEFAULT_DATA_OPEX_CLIENT_ATTEMPTS
      = 2;

  /** default initial sleep is 100ms. */
  public static final int DEFAULT_DATA_OPEX_CLIENT_BACKOFF_INIITIAL
      = 100;

  /** default backoff factor is 4. */
  public static final int DEFAULT_DATA_OPEX_CLIENT_BACKOFF_FACTOR
      = 4;

  /** default sleep limit is 30 sec. */
  public static final int DEFAULT_DATA_OPEX_CLIENT_BACKOFF_LIMIT
      = 30 * 1000;

  // Configuration key names and constants used by opex service and client.

  /** the name of this service in the service discovery. */
  public static final String OPERATION_EXECUTOR_SERVICE_NAME
      = "opex-service";

  /** for convenience of having it in this packagae. */
  public static final String CFG_ZOOKEEPER_ENSEMBLE =
      com.continuuity.common.conf.Constants.CFG_ZOOKEEPER_ENSEMBLE;
}
