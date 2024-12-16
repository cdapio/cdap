package io.cdap.cdap.logging.appender;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.logs.ILogAppenderContext;
import java.util.Collections;
import java.util.Map;

public class DefaultILogAppenderContext implements ILogAppenderContext {

  private static final String LOG_APPENDER_PREFIX = "i.log.appender.provider";
  private final Map<String, String> properties;

  /**
   * Constructs a DefaultILogAppenderContext with configuration properties specific to the
   * provider.
   *
   * @param cConf        The configuration object containing the properties.
   * @param providerName The name of the log appender provider.
   */
  public DefaultILogAppenderContext(CConfiguration cConf, String providerName) {
    String prefix = String.format("%s.%s.", LOG_APPENDER_PREFIX, providerName);
    this.properties = Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix));
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }
}
