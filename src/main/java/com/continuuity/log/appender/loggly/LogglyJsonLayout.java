package com.continuuity.log.appender.loggly;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.LayoutBase;
import com.google.common.collect.Maps;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Map;

/**
 *
 */
public class LogglyJsonLayout extends LayoutBase<ILoggingEvent> {
  private static final Logger Log = LoggerFactory.getLogger(
    LogglyJsonLayout.class
  );

  /**
   * Instance of JSON object mapper.
   */
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public String doLayout(ILoggingEvent event) {
    Map<String, Object> log = Maps.newHashMap();
    String json = null;
    try {
      log.put("description", event.getFormattedMessage());
      log.put("level", event.getLevel().levelStr);
      log.put("loggername", event.getLoggerName());
      json = mapper.writeValueAsString(log);
    } catch (IOException e) {
      Log.error("Failed writing layout for log message {}", e.getMessage());
    }
    return json;
  }
}
