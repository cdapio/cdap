package io.cdap.cdap.internal.ai;

import io.cdap.cdap.ai.spi.AIProviderContext;
import io.cdap.cdap.common.conf.CConfiguration;
import java.util.Collections;
import java.util.Map;

public class DefaultAIProviderContext implements AIProviderContext {

  public static final String AI_PROVIDER_PREFIX = "ai.provider";

  private final Map<String, String> properties;

  protected DefaultAIProviderContext(CConfiguration cConf, String providerName) {
    String prefix = String.format("%s.%s.", AI_PROVIDER_PREFIX, providerName);
    this.properties = Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix));
  }

  @Override
  public Map<String, String> getProperties() {
    return this.properties;
  }
}
