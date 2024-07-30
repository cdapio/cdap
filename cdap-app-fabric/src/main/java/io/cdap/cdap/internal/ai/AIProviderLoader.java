package io.cdap.cdap.internal.ai;

import io.cdap.cdap.ai.spi.AIProvider;
import java.util.Map;

public interface AIProviderLoader {

  Map<String, AIProvider> loadProviders();
}
