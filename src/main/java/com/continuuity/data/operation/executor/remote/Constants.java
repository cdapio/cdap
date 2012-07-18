package com.continuuity.data.operation.executor.remote;

public class Constants {
  /**
   * Configuration key names used by opex service.
   */
  public static final String CFG_DATA_OPEX_SERVER_PORT = "data.opex.server.port";
  public static final String CFG_DATA_OPEX_SERVER_ADDRESS = "data.opex.server.address";
  public static final String CFG_DATA_OPEX_SERVER_THREADS = "data.opex.server.threads";

  /**
   * property names for opex client
   */
  public static final String CFG_DATA_OPEX_CLIENT_COUNT = "data.opex.client.count";
  public static final String CFG_DATA_OPEX_CLIENT_PROVIDER = "data.opex.client.provider";

  /**
   * Default constants defined for opex service
   */
  public static final int    DEFAULT_DATA_OPEX_SERVER_PORT = 15165;
  public static final String DEFAULT_DATA_OPEX_SERVER_ADDRESS = "0.0.0.0";
  public static final int    DEFAULT_DATA_OPEX_SERVER_THREADS = 20;

  /**
   * Defaults for opex client
   */
  public static final int    DEFAULT_DATA_OPEX_CLIENT_COUNT = 5;
  public static final String DEFAULT_DATA_OPEX_CLIENT_PROVIDER = "pool";

  /** the name of this service in the service discovery */
  public static final String OPERATION_EXECUTOR_SERVICE_NAME = "opex-service";

  /** for convenience of having it in this packagae */
  public static final String CFG_ZOOKEEPER_ENSEMBLE =
      com.continuuity.common.conf.Constants.CFG_ZOOKEEPER_ENSEMBLE;

}
