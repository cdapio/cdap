package io.cdap.cdap.messaging.spi;

import java.util.Map;

public interface MessagingContext {

  /**
   * System properties are derived from the CDAP configuration. Anything in the CDAP configuration
   * will be added as an entry in the system properties.
   *
   * @return unmodifiable system properties for the messaging service.
   */
  Map<String, String> getProperties();

}
