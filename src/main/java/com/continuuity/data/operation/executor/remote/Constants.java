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

  // metrics names
  public static final String METRIC_SUCCESS = "success";
  public static final String METRIC_FAILURE = "failure";
  public static final String METRIC_REQUESTS = "requests";
  public static final String METRIC_BATCH_REQUESTS = "batchRequests";
  public static final String METRIC_BATCH_LATENCY = "batchLatency";
  public static final String METRIC_DEQUEUE_REQUESTS = "dequeueRequests";
  public static final String METRIC_DEQUEUE_LATENCY = "dequeueLatency";
  public static final String METRIC_GETGROUPID_REQUESTS = "getGroupIdRequests";
  public static final String METRIC_GETGROUPID_LATENCY = "getGroupIdLatency";
  public static final String METRIC_GETQUEUEMETA_REQUESTS = "getQueueMetaRequests";
  public static final String METRIC_GETQUEUEMETA_LATENCY = "getQueueMetaLatency";
  public static final String METRIC_CLEARFABRIC_REQUESTS = "clearFabricRequests";
  public static final String METRIC_CLEARFABRIC_LATENCY = "clearFabricLatency";
  public static final String METRIC_READKEY_REQUESTS = "readKeyRequests";
  public static final String METRIC_READKEY_LATENCY = "readKeyLatency";
  public static final String METRIC_READ_REQUESTS = "readRequests";
  public static final String METRIC_READ_LATENCY = "readLatency";
  public static final String METRIC_READCOLUMNRANGE_REQUESTS = "readColumnRangeRequests";
  public static final String METRIC_READCOLUMNRANGE_LATENCY = "readColumnRangeLatency";
  public static final String METRIC_READALLKEYS_REQUESTS = "readAllKeysRequests";
  public static final String METRIC_READALLKEYS_LATENCY = "readAllKeysLatency";
  public static final String METRIC_WRITE_REQUESTS = "writeRequests";
  public static final String METRIC_WRITE_LATENCY = "writeLatency";
  public static final String METRIC_DELETE_REQUESTS = "deleteRequests";
  public static final String METRIC_DELETE_LATENCY = "deleteLatency";
  public static final String METRIC_INCREMENT_REQUESTS = "incrementRequests";
  public static final String METRIC_INCREMENT_LATENCY = "incrementLatency";
  public static final String METRIC_COMPAREANDSWAP_REQUESTS = "compareAndSwapRequests";
  public static final String METRIC_COMPAREANDSWAP_LATENCY = "compareAndSwapLatency";
  public static final String METRIC_ENQUEUE_REQUESTS = "enqueueRequests";
  public static final String METRIC_ENQUEUE_LATENCY = "enqueueLatency";
  public static final String METRIC_ACK_REQUESTS = "ackRequests";
  public static final String METRIC_ACK_LATENCY = "ackLatency";
}
