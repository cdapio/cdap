package io.cdap.cdap.internal.ai;

import com.google.inject.Inject;
import io.cdap.cdap.ai.spi.AIProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AIProviderExtensionLoader extends AbstractExtensionLoader<String, AIProvider> implements AIProviderLoader {
  private static final Logger LOG = LoggerFactory.getLogger(AIProviderExtensionLoader.class);

  private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();
  private static final Set<String> ALLOWED_PACKAGES = createPackageSets(ALLOWED_RESOURCES);

  public static final String AI_PROVIDER_ENABLED = "ai.provider.enabled";
  public static final String AI_PROVIDER_EXTENSIONS_DIR = "ai.provider.extensions.dir";

  private final boolean aiProviderEnabled;

  @Inject
  public AIProviderExtensionLoader(CConfiguration cConf) {
    super(cConf.get(AI_PROVIDER_EXTENSIONS_DIR) != null
        ? cConf.get(AI_PROVIDER_EXTENSIONS_DIR) : "ext/ai/gcp-vertexai");
    this.aiProviderEnabled = cConf.getBoolean(AI_PROVIDER_ENABLED);
    if (this.aiProviderEnabled == false) {
      LOG.debug("AI provider is not enabled.");
      return;
    }
    LOG.debug("AI Provider enabled.");
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(AIProvider aiServiceProvider) {
    if (!this.aiProviderEnabled) {
      LOG.debug("AI provider is not enabled for {}.", aiServiceProvider.getName());
      return Collections.emptySet();
    }
    return Collections.singleton(aiServiceProvider.getName());
  }

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
    // filter classes to provide isolation from CDAP's classes.
    return new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return ALLOWED_RESOURCES.contains(resource);
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return ALLOWED_PACKAGES.contains(packageName);
      }
    };
  }

  private static Set<String> createAllowedResources() {
    try {
      return ClassPathResources.getResourcesWithDependencies(
          AIProvider.class.getClassLoader(),
          AIProvider.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to trace dependencies for AI service provider extension. "
          + "Usage of AI service might fail.", e);
    }
  }

  @Override
  public Map<String, AIProvider> loadProviders() {
    return getAll();
  }
}
