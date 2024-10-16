package io.cdap.cdap.messaging.client;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.spi.MessagingContext;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMessagingContext implements MessagingContext {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMessagingContext.class);

  private final CConfiguration cConf;

  private static final String storageImpl = "gcp-spanner";

  DefaultMessagingContext(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public Map<String, String> getProperties() {
    String propertiesPrefix =
        Constants.Dataset.STORAGE_EXTENSION_PROPERTY_PREFIX + storageImpl + ".";
    LOG.info("Properties prefix {}", propertiesPrefix);
    return Collections.unmodifiableMap(cConf.getPropsWithPrefix(propertiesPrefix));
  }
}
