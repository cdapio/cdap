package io.cdap.cdap.logging.appender.loader;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.logs.ILogAppender;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension loader for {@link ILogAppender} implementations.
 */
public class ILogAppenderExtensionLoader extends AbstractExtensionLoader<String, ILogAppender> {

  private static final Logger LOG = LoggerFactory.getLogger(ILogAppenderExtensionLoader.class);
  private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();
  private static final Set<String> ALLOWED_PACKAGES = createPackageSets(ALLOWED_RESOURCES);

  private static final String LOG_APPENDER_ENABLED = "i.log.appender.provider.enabled";
  private static final String LOG_APPENDER_EXTENSIONS_DIR = "i.log.appender.provider.extensions.dir";

  private final boolean isLogAppenderEnabled;

  @Inject
  public ILogAppenderExtensionLoader(CConfiguration cConf) {
    super(cConf.get(LOG_APPENDER_EXTENSIONS_DIR));
    this.isLogAppenderEnabled = cConf.getBoolean(LOG_APPENDER_ENABLED);

    if (this.isLogAppenderEnabled) {
      LOG.debug("ILogAppender is enabled.");
    } else {
      LOG.debug("ILogAppender is not enabled.");
    }
  }

  private static Set<String> createAllowedResources() {
    try {
      return ClassPathResources.getResourcesWithDependencies(ILogAppender.class.getClassLoader(),
          ILogAppender.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to trace dependencies for ILogAppender extension.", e);
    }
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(ILogAppender iLogAppender) {
    if (!isLogAppenderEnabled) {
      return Collections.emptySet();
    }
    return Collections.singleton(iLogAppender.getName());
  }

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
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
}
