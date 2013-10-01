/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.logging.common;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.logging.LogEvent;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of LogWriter that writes to Flume.
 */
public class FlumeLogWriter implements LogWriter {
  private static final Logger LOG
    = LoggerFactory.getLogger(FlumeLogWriter.class);
  RpcClient client;
  private final int port;
  private final String hostname;

  public FlumeLogWriter(CConfiguration configuration) {
    port = configuration.getInt(Constants.CFG_LOG_COLLECTION_PORT,
                                 Constants.DEFAULT_LOG_COLLECTION_PORT);
    hostname = configuration.get(Constants.CFG_LOG_COLLECTION_SERVER_ADDRESS,
                                 Constants.DEFAULT_LOG_COLLECTION_SERVER_ADDRESS);
    client = RpcClientFactory.getDefaultInstance(hostname, port, 1);
  }

  @Override
  public boolean write(String tag, String level, String message) {
    SimpleEvent event = new SimpleEvent();
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(LogEvent.FIELD_NAME_LOGTAG, tag);
    headers.put(LogEvent.FIELD_NAME_LOGLEVEL, level);
    event.setHeaders(headers);
    event.setBody(message.getBytes());
    try {
      if (!client.isActive()) {
        client.close();
        client = RpcClientFactory.getDefaultInstance(hostname, port, 1);
      }
      if (client.isActive()) {
        client.append(event);
      } else {
        LOG.warn("Unable to send log to central log server. Check the server.");
      }
    } catch (EventDeliveryException e) {
      LOG.warn("Failed to send log event. Reason : {}", e.getMessage(), e);
      return false;
    }
    return true;
  }
}
