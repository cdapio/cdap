package com.continuuity.common.logging;

import com.continuuity.api.common.LogDispatcher;
import com.continuuity.api.common.LogTag;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.StackTraceUtil;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
@Deprecated
public class FlumeLogDispatcher extends AbstractLogDispatcher {
  private static final Logger LOG
    = LoggerFactory.getLogger(FlumeLogDispatcher.class);
  private final CConfiguration configuration;
  RpcClient client;
  private final SimpleDateFormat date;
  private final int port;
  private final String hostname;

  public FlumeLogDispatcher(CConfiguration configuration) {
    this.configuration = configuration;
    port = configuration.getInt(Constants.CFG_LOG_COLLECTION_PORT,
                                    Constants.DEFAULT_LOG_COLLECTION_PORT);
    hostname = configuration.get(Constants.CFG_LOG_COLLECTION_SERVER_ADDRESS,
                                        Constants.DEFAULT_LOG_COLLECTION_SERVER_ADDRESS);
    this.date = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    client = RpcClientFactory.getDefaultInstance(hostname, port, 1);
  }

  @Override
  public String getName() {
    return "flume-log-dispatcher";
  }

  @Override
  public boolean writeLog(LogTag tag, LogDispatcher.Level level, String message,
                          String stack) {
    SimpleEvent event = new SimpleEvent();
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(LogEvent.FIELD_NAME_LOGTAG, tag.getTag());
    headers.put(LogEvent.FIELD_NAME_LOGLEVEL, level.name());
    String msg = String.format("%s: %s", date.format(new Date()), message);
    if(stack != null) {
      msg = String.format("%s: %s. Stack trace : %s",date.format(new Date()),
                          message, stack);
    }
    event.setHeaders(headers);
    event.setBody(msg.getBytes());
    try {
      if(!client.isActive()) {
        client.close();
        client = RpcClientFactory.getDefaultInstance(hostname, port, 1);
      }
      if(client.isActive()) {
        client.append(event);
      } else {
        LOG.warn("Unable to send log to central log server. Check the server.");
      }
    } catch (EventDeliveryException e) {
      LOG.warn("Failed to send log event. Reason : {}", e.getMessage());
      LOG.warn(StackTraceUtil.toStringStackTrace(e));
      return false;
    }
    return true;
  }
}
