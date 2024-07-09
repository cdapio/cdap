package io.cdap.cdap.internal.ai;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.cdap.cdap.ai.spi.AIProvider;

public class AIProviderModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(AIProvider.class).to(DefaultAIProvider.class).in(Scopes.SINGLETON);
    bind(AIProviderLoader.class).to(AIProviderExtensionLoader.class);
  }
}
